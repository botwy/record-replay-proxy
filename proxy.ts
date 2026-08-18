import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

import {
  createProxyMiddleware,
  responseInterceptor,
} from 'http-proxy-middleware';

import WebSocket, {
  WebSocketServer,
} from 'ws';

import picomatch from 'picomatch';

/*
 * ============================================================
 * TYPES
 * ============================================================
 */

type Config = {
  target: string;
  port: number;

  tls?: {
    rejectUnauthorized?: boolean;
  };

  record: {
    include: string[];
    exclude?: string[];
    contentTypes: string[];
  };
};

type HttpRecording = {
  key: string;
  method: string;
  url: string;
  requestBody: string;

  response: {
    status: number;
    headers: Record<string, string>;
    body: string;
    bodyEncoding?: 'utf8' | 'base64';
  };
};

type WsRecording = {
  id: string;
  url: string;
  binary: boolean;
  data: string;
  dataEncoding?: 'utf8' | 'base64';
  createdAt: string;
};

type RequestWithBody =
  http.IncomingMessage & {
    __mockBodyBuffer?: Buffer;
  };

/*
 * ============================================================
 * CONFIG
 * ============================================================
 */

const __filename =
  fileURLToPath(import.meta.url);

const __dirname =
  path.dirname(__filename);

const configPath =
  path.join(
    __dirname,
    'config.json',
  );

const config: Config =
  JSON.parse(
    fs.readFileSync(
      configPath,
      'utf8',
    ),
  );

const command =
  process.argv[2] ?? 'replay';

const validCommands = [
  'record',
  'replay',
  'ws:list',
  'ws:send',
  'rest:list',
];

if (!validCommands.includes(command)) {
  console.error(`
Usage:
  npm run record
  npm run replay
  npm run ws:list
  npm run ws:send -- <id>
  npm run rest:list
`);

  process.exit(1);
}

const mode =
  command === 'record'
    ? 'record'
    : 'replay';

const PORT =
  config.port;

const TARGET =
  config.target;

const rejectUnauthorized =
  config.tls?.rejectUnauthorized ?? true;

/*
 * ============================================================
 * DIRECTORIES
 * ============================================================
 */

const recordingsDir =
  path.join(
    __dirname,
    'recordings',
  );

const httpDir =
  path.join(
    recordingsDir,
    'http',
  );

const wsDir =
  path.join(
    recordingsDir,
    'ws',
  );

fs.mkdirSync(
  httpDir,
  {
    recursive: true,
  },
);

fs.mkdirSync(
  wsDir,
  {
    recursive: true,
  },
);

/*
 * ============================================================
 * MATCHERS
 * ============================================================
 */

const includeMatcher =
  picomatch(
    config.record.include,
  );

const excludeMatcher =
  picomatch(
    config.record.exclude ?? [],
  );

/*
 * ============================================================
 * RECORD SCOPE
 * ============================================================
 */

function isInRecordScope(
  requestUrl: string,
): boolean {
  const url =
    new URL(
      requestUrl,
      TARGET,
    );

  const pathname =
    url.pathname;

  if (!includeMatcher(pathname)) {
    return false;
  }

  if (excludeMatcher(pathname)) {
    return false;
  }

  return true;
}

/*
 * ============================================================
 * CONTENT TYPE
 * ============================================================
 */

function shouldRecordResponse(
  requestUrl: string,
  contentType: string,
): boolean {
  if (
    !isInRecordScope(
      requestUrl,
    )
  ) {
    return false;
  }

  const normalized =
    contentType.toLowerCase();

  return config.record.contentTypes.some(
    type =>
      normalized.includes(
        type.toLowerCase(),
      ),
  );
}

function isJsonContentType(
  contentType: string,
): boolean {
  const normalized =
    contentType.toLowerCase();

  return (
    normalized.includes(
      'application/json',
    ) ||
    normalized.includes(
      '+json',
    )
  );
}

/*
 * ============================================================
 * HELPERS
 * ============================================================
 */

function sha256(
  value: string,
): string {
  return crypto
    .createHash('sha256')
    .update(value)
    .digest('hex');
}

function makeRequestKey(
  method: string,
  url: string,
  body: string,
): string {
  return sha256(
    [
      method.toUpperCase(),
      url,
      body,
    ].join('\n'),
  );
}

function readBody(
  req: http.IncomingMessage,
) {
  return new Promise(
    (
      resolve,
      reject,
    ) => {
      const chunks: Buffer[] = [];

      req.on(
        'data',
        chunk => {
          chunks.push(
            Buffer.isBuffer(chunk)
              ? chunk
              : Buffer.from(chunk),
          );
        },
      );

      req.on(
        'end',
        () => {
          resolve(
            Buffer.concat(chunks),
          );
        },
      );

      req.on(
        'error',
        reject,
      );
    },
  );
}

function headersToObject(
  headers: http.IncomingHttpHeaders,
) {
  const result: Record<string, string> = {};

  for (
    const [
      key,
      value,
    ] of Object.entries(headers)
  ) {
    if (value === undefined) {
      continue;
    }

    result[key] =
      Array.isArray(value)
        ? value.join(', ')
        : value;
  }

  return result;
}

function sendJson(
  res: http.ServerResponse,
  status: number,
  data: unknown,
) {
  const body =
    JSON.stringify(
      data,
      null,
      2,
    );

  res.writeHead(
    status,
    {
      'content-type':
        'application/json; charset=utf-8',

      'content-length':
        String(
          Buffer.byteLength(body),
        ),
    },
  );

  res.end(body);
}

function makeWsId(
  createdAt: string,
): string {
  const iso =
    new Date(createdAt)
      .toISOString()
      .replace(/:/g, '-')
      .replace(/\./g, '-');

  const suffix =
    crypto
      .randomBytes(4)
      .toString('hex');

  return `${iso}-${suffix}`;
}

/*
 * ============================================================
 * REST REQUEST BODY
 * ============================================================
 *
 * We read the body before calling the proxy because:
 *
 *   record/replay
 *
 * needs the body to calculate the request key.
 *
 * The stream is therefore consumed.
 *
 * We put the Buffer onto req and write it back to the
 * outgoing proxy request inside on.proxyReq.
 *
 * ============================================================
 */

function writeProxyRequestBody(
  proxyReq: http.ClientRequest,
  req: RequestWithBody,
) {
  const body =
    req.__mockBodyBuffer;

  if (
    !body ||
    body.length === 0
  ) {
    return;
  }

  proxyReq.setHeader(
    'content-length',
    String(body.length),
  );

  proxyReq.write(body);
}

/*
 * ============================================================
 * REST PROXY
 * ============================================================
 *
 * This is the only HTTP proxy implementation.
 *
 * No http-proxy.
 * No proxy.once().
 * No manual proxy.web().
 *
 * ============================================================
 */

const restProxy =
  createProxyMiddleware({
    target: TARGET,

    changeOrigin: true,

    secure:
      rejectUnauthorized,

    /*
     * We need the complete response only while recording.
     *
     * In replay mode this proxy is used only for fallback
     * requests, so target responses pass through normally.
     */

    selfHandleResponse:
      mode === 'record',

    on: {
      /*
       * ========================================================
       * PROXY REQUEST
       * ========================================================
       */

      proxyReq: (
        proxyReq,
        req,
      ) => {
        const request =
          req as RequestWithBody;

        writeProxyRequestBody(
          proxyReq,
          request,
        );

        console.log(
          `[HTTP → TARGET] ${
            req.method ?? 'GET'
          } ${req.url ?? '/'}`,
        );
      },

      /*
       * ========================================================
       * PROXY RESPONSE
       * ========================================================
       *
       * responseInterceptor gives us the COMPLETE response
       * body as Buffer.
       *
       * We don't subscribe to proxyRes.on('data').
       *
       * We don't use proxy.once().
       *
       * ========================================================
       */

      proxyRes:
        mode === 'record'
          ? responseInterceptor(
              async (
                responseBuffer,
                proxyRes,
                req,
              ) => {
                const request =
                  req as RequestWithBody;

                const method =
                  req.method ?? 'GET';

                const url =
                  req.url ?? '/';

                const contentType =
                  String(
                    proxyRes.headers[
                      'content-type'
                    ] ?? '',
                  );

                /*
                 * contentTypes still controls WHAT is recorded.
                 *
                 * If the response isn't in scope, return it
                 * untouched.
                 */

                if (
                  !shouldRecordResponse(
                    url,
                    contentType,
                  )
                ) {
                  return responseBuffer;
                }

                const requestBodyBuffer =
                  request.__mockBodyBuffer ??
                  Buffer.alloc(0);

                const requestBody =
                  requestBodyBuffer.toString(
                    'utf8',
                  );

                const isJson =
                  isJsonContentType(
                    contentType,
                  );

                /*
                 * IMPORTANT:
                 *
                 * JSON is stored as a normal UTF-8 string.
                 *
                 * We do NOT JSON.parse() it.
                 *
                 * Therefore if target returns:
                 *
                 * {"foo":"bar"}
                 *
                 * the recording contains:
                 *
                 * "body": "{\"foo\":\"bar\"}"
                 *
                 * which is a JSON string containing the
                 * original JSON text.
                 */

                const storedBody =
                  isJson
                    ? responseBuffer.toString(
                        'utf8',
                      )
                    : responseBuffer.toString(
                        'base64',
                      );

                const headers =
                  headersToObject(
                    proxyRes.headers,
                  );

                /*
                 * These headers describe the original
                 * transport response, not our stored response.
                 *
                 * In particular content-length can become
                 * invalid after decompression/interception.
                 */

                delete headers[
                  'content-length'
                ];

                delete headers[
                  'transfer-encoding'
                ];

                delete headers[
                  'content-encoding'
                ];

                const key =
                  makeRequestKey(
                    method,
                    url,
                    requestBody,
                  );

                const recording:
                  HttpRecording = {
                    key,

                    method,

                    url,

                    requestBody,

                    response: {
                      status:
                        proxyRes.statusCode ??
                        500,

                      headers,

                      body:
                        storedBody,

                      bodyEncoding:
                        isJson
                          ? 'utf8'
                          : 'base64',
                    },
                  };

                const filename =
                  path.join(
                    httpDir,
                    `${key}.json`,
                  );

                fs.writeFileSync(
                  filename,
                  JSON.stringify(
                    recording,
                    null,
                    2,
                  ),
                );

                console.log(
                  `[HTTP RECORD] ${method} ${url}`,
                );

                /*
                 * Return the original response.
                 *
                 * The browser receives exactly what the
                 * target returned.
                 */

                return responseBuffer;
              },
            )
          : undefined,

      /*
       * ========================================================
       * PROXY ERROR
       * ========================================================
       */

      error: (
        error,
        _req,
        res,
      ) => {
        console.error(
          '[PROXY ERROR]',
          error.message,
        );

        if (
          res &&
          !res.headersSent
        ) {
          res.writeHead(502);
          res.end(
            'Proxy error',
          );
        }
      },
    },
  });

/*
 * ============================================================
 * REST RECORD
 * ============================================================
 */

async function recordHttp(
  req: http.IncomingMessage,
  res: http.ServerResponse,
) {
  const request =
    req as RequestWithBody;

  const bodyBuffer =
    await readBody(req);

  request.__mockBodyBuffer =
    bodyBuffer;

  restProxy(
    req,
    res,
  );
}

/*
 * ============================================================
 * REST REPLAY
 * ============================================================
 */

async function replayHttp(
  req: http.IncomingMessage,
  res: http.ServerResponse,
) {
  const request =
    req as RequestWithBody;

  const bodyBuffer =
    await readBody(req);

  request.__mockBodyBuffer =
    bodyBuffer;

  const body =
    bodyBuffer.toString(
      'utf8',
    );

  const method =
    req.method ?? 'GET';

  const url =
    req.url ?? '/';

  /*
   * ========================================================
   * 1. OUTSIDE INCLUDE / EXCLUDE
   * ========================================================
   *
   * Never search recordings.
   * Always proxy to target.
   */

  if (
    !isInRecordScope(url)
  ) {
    console.log(
      `[HTTP → TARGET] ${method} ${url}`,
    );

    restProxy(
      req,
      res,
    );

    return;
  }

  /*
   * ========================================================
   * 2. FIND RECORDING
   * ========================================================
   */

  const key =
    makeRequestKey(
      method,
      url,
      body,
    );

  const filename =
    path.join(
      httpDir,
      `${key}.json`,
    );

  /*
   * ========================================================
   * 3. RECORDING MISS → TARGET
   * ========================================================
   */

  if (
    !fs.existsSync(
      filename,
    )
  ) {
    console.log(
      `[HTTP MISS → TARGET] ${method} ${url}`,
    );

    restProxy(
      req,
      res,
    );

    return;
  }

  /*
   * ========================================================
   * 4. REPLAY
   * ========================================================
   */

  const recording:
    HttpRecording =
    JSON.parse(
      fs.readFileSync(
        filename,
        'utf8',
      ),
    );

  const encoding =
    recording.response.bodyEncoding ??
    'base64';

  const responseBody =
    Buffer.from(
      recording.response.body,
      encoding,
    );

  const headers = {
    ...recording.response.headers,
  };

  delete headers.connection;
  delete headers['transfer-encoding'];
  delete headers['content-encoding'];

  headers['content-length'] =
    String(
      responseBody.length,
    );

  console.log(
    `[HTTP REPLAY] ${method} ${url}`,
  );

  res.writeHead(
    recording.response.status,
    headers,
  );

  res.end(
    responseBody,
  );
}

/*
 * ============================================================
 * WEBSOCKET REPLAY
 * ============================================================
 *
 * WS intentionally remains implemented with `ws`.
 *
 * This gives us:
 *
 *   message
 *   binary
 *   send
 *   close
 *
 * and therefore keeps ws:list / ws:send working exactly
 * as before.
 *
 * ============================================================
 */

const replayWss =
  new WebSocketServer({
    noServer: true,
  });

const replayClients =
  new Set<WebSocket>();

replayWss.on(
  'connection',
  ws => {
    console.log(
      '[WS] client connected',
    );

    replayClients.add(ws);

    ws.on(
      'close',
      () => {
        replayClients.delete(ws);

        console.log(
          '[WS] client disconnected',
        );
      },
    );

    ws.on(
      'error',
      () => {
        replayClients.delete(ws);
      },
    );

    ws.on(
      'message',
      (
        data,
        isBinary,
      ) => {
        console.log(
          '[WS CLIENT → PROXY]',
          isBinary
            ? '<binary>'
            : data.toString(),
        );
      },
    );
  },
);

/*
 * ============================================================
 * WEBSOCKET RECORD SERVER
 * ============================================================
 */

const recordWss =
  new WebSocketServer({
    noServer: true,
  });

recordWss.on(
  'connection',
  (
    browserWs,
    req,
  ) => {
    recordWebSocket(
      req,
      browserWs,
    );
  },
);

/*
 * ============================================================
 * RECORD WEBSOCKET
 * ============================================================
 */

function recordWebSocket(
  req: http.IncomingMessage,
  browserWs: WebSocket,
) {
  const targetUrl =
    new URL(TARGET);

  const wsProtocol =
    targetUrl.protocol === 'https:'
      ? 'wss:'
      : 'ws:';

  const wsUrl =
    `${wsProtocol}//${targetUrl.host}${req.url ?? '/'}`;

  const headers:
    Record<string, string> = {};

  for (
    const [
      key,
      value,
    ] of Object.entries(
      req.headers,
    )
  ) {
    if (value === undefined) {
      continue;
    }

    /*
     * These headers are generated by the WebSocket
     * handshake and shouldn't be copied.
     */

    if (
      key === 'host' ||
      key === 'connection' ||
      key === 'upgrade' ||
      key === 'sec-websocket-key' ||
      key === 'sec-websocket-version' ||
      key === 'sec-websocket-extensions' ||
      key === 'sec-websocket-protocol'
    ) {
      continue;
    }

    headers[key] =
      Array.isArray(value)
        ? value.join(', ')
        : value;
  }

  const protocolHeader =
    req.headers[
      'sec-websocket-protocol'
    ];

  const protocols =
    protocolHeader
      ? String(protocolHeader)
          .split(',')
          .map(
            value =>
              value.trim(),
          )
      : undefined;

  const targetWs =
    new WebSocket(
      wsUrl,
      protocols,
      {
        headers,

        rejectUnauthorized:
          rejectUnauthorized,
      },
    );

  targetWs.on(
    'open',
    () => {
      console.log(
        `[WS RECORD] connected ${wsUrl}`,
      );

      /*
       * Browser → target
       */

      browserWs.on(
        'message',
        (
          data,
          isBinary,
        ) => {
          if (
            targetWs.readyState !==
            WebSocket.OPEN
          ) {
            return;
          }

          targetWs.send(
            data,
            {
              binary: isBinary,
            },
          );
        },
      );

      /*
       * Target → browser
       *
       * This is also where individual WS messages
       * are recorded.
       */

      targetWs.on(
        'message',
        (
          data,
          isBinary,
        ) => {
          if (
            browserWs.readyState ===
            WebSocket.OPEN
          ) {
            browserWs.send(
              data,
              {
                binary: isBinary,
              },
            );
          }

          saveWsMessage(
            req.url ?? '/',
            data,
            isBinary,
          );
        },
      );
    },
  );

  targetWs.on(
    'error',
    error => {
      console.error(
        '[WS TARGET ERROR]',
        error.message,
      );

      if (
        browserWs.readyState ===
        WebSocket.OPEN
      ) {
        browserWs.close(
          1011,
          'Target WebSocket error',
        );
      }
    },
  );

  targetWs.on(
    'close',
    (
      code,
      reason,
    ) => {
      console.log(
        `[WS TARGET CLOSE] ${code}`,
      );

      if (
        browserWs.readyState ===
        WebSocket.OPEN
      ) {
        browserWs.close(
          code,
          reason,
        );
      }
    },
  );

  browserWs.on(
    'close',
    () => {
      if (
        targetWs.readyState ===
        WebSocket.OPEN
      ) {
        targetWs.close();
      }
    },
  );

  browserWs.on(
    'error',
    () => {
      if (
        targetWs.readyState ===
        WebSocket.OPEN
      ) {
        targetWs.close();
      }
    },
  );
}

/*
 * ============================================================
 * SAVE WS MESSAGE
 * ============================================================
 */

function saveWsMessage(
  url: string,
  data: WebSocket.RawData,
  binary: boolean,
) {
  const createdAt =
    new Date().toISOString();

  const id =
    makeWsId(
      createdAt,
    );

  const buffer =
    Buffer.isBuffer(data)
      ? data
      : Buffer.from(
          data as ArrayBuffer,
        );

  const recording:
    WsRecording = {
      id,

      url,

      binary,

      data:
        binary
          ? buffer.toString(
              'base64',
            )
          : buffer.toString(
              'utf8',
            ),

      dataEncoding:
        binary
          ? 'base64'
          : 'utf8',

      createdAt,
    };

  const filename =
    path.join(
      wsDir,
      `${id}.json`,
    );

  fs.writeFileSync(
    filename,
    JSON.stringify(
      recording,
      null,
      2,
    ),
  );

  console.log(
    `[WS RECORD] ${id}`,
  );
}

/*
 * ============================================================
 * WS LIST
 * ============================================================
 */

function getWsRecordings():
  WsRecording[] {
  if (
    !fs.existsSync(wsDir)
  ) {
    return [];
  }

  return fs
    .readdirSync(wsDir)
    .filter(
      file =>
        file.endsWith('.json'),
    )
    .sort()
    .map(
      file =>
        JSON.parse(
          fs.readFileSync(
            path.join(
              wsDir,
              file,
            ),
            'utf8',
          ),
        ),
    );
}

function printWsList() {
  const recordings =
    getWsRecordings();

  if (
    recordings.length === 0
  ) {
    console.log(
      'No WebSocket recordings found.',
    );

    return;
  }

  for (
    const recording
    of recordings
  ) {
    console.log('');

    console.log(
      `ID:          ${recording.id}`,
    );

    console.log(
      `URL:         ${recording.url}`,
    );

    console.log(
      `Binary:      ${recording.binary}`,
    );

    console.log(
      `Encoding:    ${
        recording.dataEncoding ??
        (
          recording.binary
            ? 'base64'
            : 'utf8'
        )
      }`,
    );

    console.log(
      `Created:     ${recording.createdAt}`,
    );

    console.log(
      'Message:',
    );

    if (
      recording.binary
    ) {
      console.log(
        '<binary>',
      );
    } else {
      console.log(
        recording.data,
      );
    }
  }

  console.log('');

  console.log(
    `Total: ${recordings.length}`,
  );
}

/*
 * ============================================================
 * REST LIST
 * ============================================================
 */

function printRestList() {
  if (
    !fs.existsSync(httpDir)
  ) {
    console.log(
      'No REST recordings found.',
    );

    return;
  }

  const files =
    fs
      .readdirSync(httpDir)
      .filter(
        file =>
          file.endsWith('.json'),
      )
      .sort();

  if (
    files.length === 0
  ) {
    console.log(
      'No REST recordings found.',
    );

    return;
  }

  for (
    const file
    of files
  ) {
    const filename =
      path.join(
        httpDir,
        file,
      );

    const recording:
      HttpRecording =
      JSON.parse(
        fs.readFileSync(
          filename,
          'utf8',
        ),
      );

    const contentType =
      recording.response.headers[
        'content-type'
      ] ?? '';

    console.log('');

    console.log(
      `Key:          ${recording.key}`,
    );

    console.log(
      `Method:       ${recording.method}`,
    );

    console.log(
      `URL:          ${recording.url}`,
    );

    console.log(
      `Status:       ${recording.response.status}`,
    );

    console.log(
      `Content-Type: ${contentType}`,
    );

    console.log(
      `Encoding:     ${
        recording.response.bodyEncoding ??
        'base64'
      }`,
    );

    console.log(
      `File:         ${file}`,
    );

    console.log(
      'Request body:',
    );

    console.log(
      recording.requestBody ||
      '<empty>',
    );

    console.log(
      'Response body:',
    );

    if (
      recording.response.bodyEncoding ===
      'base64'
    ) {
      console.log(
        '<base64>',
      );
    } else {
      console.log(
        recording.response.body,
      );
    }
  }

  console.log('');

  console.log(
    `Total: ${files.length}`,
  );
}

/*
 * ============================================================
 * WS SEND
 * ============================================================
 */

async function sendWsCommand(
  id: string,
) {
  const url =
    `http://127.0.0.1:${PORT}` +
    `/__mock/ws/send/` +
    encodeURIComponent(id);

  try {
    const response =
      await fetch(
        url,
        {
          method: 'POST',
        },
      );

    const body =
      await response.text();

    console.log(body);

    if (!response.ok) {
      process.exit(1);
    }
  } catch (error) {
    console.error(
      'Cannot connect to replay proxy.',
    );

    console.error(
      String(error),
    );

    process.exit(1);
  }
}

/*
 * ============================================================
 * CLI COMMANDS
 * ============================================================
 */

if (
  command === 'ws:list'
) {
  printWsList();
  process.exit(0);
}

if (
  command === 'rest:list'
) {
  printRestList();
  process.exit(0);
}

if (
  command === 'ws:send'
) {
  const id =
    process.argv[3];

  if (!id) {
    console.error(
      'Usage: npm run ws:send -- <id>',
    );

    process.exit(1);
  }

  await sendWsCommand(id);

  process.exit(0);
}

/*
 * ============================================================
 * HTTP SERVER
 * ============================================================
 */

const server =
  http.createServer(
    async (
      req,
      res,
    ) => {
      try {
        /*
         * ====================================================
         * HEALTH
         * ====================================================
         */

        if (
          req.method === 'GET' &&
          req.url ===
            '/__mock/health'
        ) {
          sendJson(
            res,
            200,
            {
              mode,

              target:
                TARGET,

              wsClients:
                replayClients.size,
            },
          );

          return;
        }

        /*
         * ====================================================
         * WS LIST API
         * ====================================================
         */

        if (
          req.method === 'GET' &&
          req.url ===
            '/__mock/ws'
        ) {
          sendJson(
            res,
            200,
            getWsRecordings(),
          );

          return;
        }

        /*
         * ====================================================
         * WS SEND API
         * ====================================================
         */

        const sendMatch =
          req.url?.match(
            /^\/__mock\/ws\/send\/(.+)$/,
          );

        if (
          req.method === 'POST' &&
          sendMatch
        ) {
          const id =
            decodeURIComponent(
              sendMatch[1],
            );

          const filename =
            path.join(
              wsDir,
              `${id}.json`,
            );

          if (
            !fs.existsSync(
              filename,
            )
          ) {
            sendJson(
              res,
              404,
              {
                error:
                  'WS recording not found',

                id,
              },
            );

            return;
          }

          const recording:
            WsRecording =
            JSON.parse(
              fs.readFileSync(
                filename,
                'utf8',
              ),
            );

          const encoding =
            recording.dataEncoding ??
            (
              recording.binary
                ? 'base64'
                : 'utf8'
            );

          const data =
            Buffer.from(
              recording.data,
              encoding,
            );

          let sent = 0;

          for (
            const client
            of replayClients
          ) {
            if (
              client.readyState !==
              WebSocket.OPEN
            ) {
              continue;
            }

            client.send(
              data,
              {
                binary:
                  recording.binary,
              },
            );

            sent++;
          }

          console.log(
            `[WS SEND] ${id} → ${sent} client(s)`,
          );

          sendJson(
            res,
            200,
            {
              ok: true,
              id,
              sent,
            },
          );

          return;
        }

        /*
         * ====================================================
         * REST
         * ====================================================
         */

        if (
          mode === 'record'
        ) {
          await recordHttp(
            req,
            res,
          );

          return;
        }

        await replayHttp(
          req,
          res,
        );
      } catch (error) {
        console.error(
          '[SERVER ERROR]',
          error,
        );

        if (
          !res.headersSent
        ) {
          sendJson(
            res,
            500,
            {
              error:
                String(error),
            },
          );
        }
      }
    },
  );

/*
 * ============================================================
 * WEBSOCKET UPGRADE
 * ============================================================
 *
 * IMPORTANT:
 *
 * WS does NOT use http-proxy-middleware here.
 *
 * RECORD:
 *   browser
 *      ↓
 *   recordWss
 *      ↓
 *   ws client → target
 *
 * REPLAY:
 *   browser
 *      ↓
 *   replayWss
 *
 * This preserves individual WS message boundaries.
 *
 * ============================================================
 */

server.on(
  'upgrade',
  (
    req,
    socket,
    head,
  ) => {
    /*
     * Admin HTTP API isn't WebSocket.
     */

    if (
      req.url?.startsWith(
        '/__mock/',
      )
    ) {
      socket.destroy();

      return;
    }

    /*
     * ========================================================
     * RECORD
     * ========================================================
     */

    if (
      mode === 'record'
    ) {
      recordWss.handleUpgrade(
        req,
        socket,
        head,
        ws => {
          recordWss.emit(
            'connection',
            ws,
            req,
          );
        },
      );

      return;
    }

    /*
     * ========================================================
     * REPLAY
     * ========================================================
     */

    replayWss.handleUpgrade(
      req,
      socket,
      head,
      ws => {
        replayWss.emit(
          'connection',
          ws,
          req,
        );
      },
    );
  },
);

/*
 * ============================================================
 * START
 * ============================================================
 */

server.listen(
  PORT,
  () => {
    console.log('');

    console.log(
      '========================================',
    );

    console.log(
      ' REST / WebSocket Record Replay Proxy',
    );

    console.log(
      '========================================',
    );

    console.log(
      `Mode:   ${mode}`,
    );

    console.log(
      `Target: ${TARGET}`,
    );

    console.log(
      `Port:   ${PORT}`,
    );

    console.log(
      `HTTP:   ${httpDir}`,
    );

    console.log(
      `WS:     ${wsDir}`,
    );

    console.log(
      '========================================',
    );

    console.log('');
  },
);