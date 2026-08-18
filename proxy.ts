import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Readable } from 'node:stream';

import httpProxy from 'http-proxy';
import WebSocket, { WebSocketServer } from 'ws';
import picomatch from 'picomatch';

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

    // JSON/text responses are stored as UTF-8 strings.
    // Binary responses are stored as base64 strings.
    body: string;

    bodyEncoding: 'utf8' | 'base64';
  };
};

type WsRecording = {
  id: string;
  url: string;
  binary: boolean;
  data: string;
  dataEncoding: 'utf8' | 'base64';
  createdAt: string;
};

/*
 * ============================================================
 * CONFIG
 * ============================================================
 */

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const configPath = path.join(
  __dirname,
  'config.json',
);

const config: Config = JSON.parse(
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

const PORT = config.port;
const TARGET = config.target;

const rejectUnauthorized =
  config.tls?.rejectUnauthorized ?? true;

/*
 * ============================================================
 * DIRECTORIES
 * ============================================================
 */

const recordingsDir = path.join(
  __dirname,
  'recordings',
);

const httpDir = path.join(
  recordingsDir,
  'http',
);

const wsDir = path.join(
  recordingsDir,
  'ws',
);

fs.mkdirSync(
  httpDir,
  { recursive: true },
);

fs.mkdirSync(
  wsDir,
  { recursive: true },
);

/*
 * ============================================================
 * GLOB MATCHERS
 * ============================================================
 */

const includeMatcher = picomatch(
  config.record.include,
);

const excludeMatcher = picomatch(
  config.record.exclude ?? [],
);

/*
 * ============================================================
 * URL MATCHING
 *
 * Host is deliberately removed.
 *
 * Example:
 *
 * https://example.com/api/users
 *
 * becomes:
 *
 * /api/users
 * ============================================================
 */

function getPathname(
  requestUrl: string,
): string {
  try {
    return new URL(
      requestUrl,
      TARGET,
    ).pathname;
  } catch {
    return requestUrl.split('?')[0];
  }
}

function isInRecordScope(
  requestUrl: string,
): boolean {
  const pathname =
    getPathname(requestUrl);

  if (!includeMatcher(pathname)) {
    return false;
  }

  if (excludeMatcher(pathname)) {
    return false;
  }

  return true;
}

function shouldRecordResponse(
  requestUrl: string,
  contentType: string,
): boolean {
  if (!isInRecordScope(requestUrl)) {
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
    (resolve, reject) => {
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
    const [key, value]
    of Object.entries(headers)
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

/*
 * ============================================================
 * HTTP REQUESTS THAT ARE CURRENTLY BEING RECORDED
 *
 * WeakMap:
 *
 * req -> request information
 *
 * proxyRes gives us the same req, so we can correctly
 * associate the response with its request even when
 * multiple requests are running simultaneously.
 * ============================================================
 */

const requestsToRecord =
  new WeakMap<
    http.IncomingMessage,
    {
      body: Buffer;
      url: string;
      method: string;
    }
  >();

/*
 * ============================================================
 * HTTP PROXY
 * ============================================================
 */

const proxy =
  httpProxy.createProxyServer({
    changeOrigin: true,
    secure: rejectUnauthorized,
  });

proxy.on(
  'error',
  (
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
      res.end('Proxy error');
    }
  },
);

/*
 * ============================================================
 * HTTP RESPONSE INTERCEPTOR
 *
 * One permanent listener.
 *
 * We intentionally DO NOT use:
 *
 * proxy.once('proxyRes', ...)
 *
 * because proxy is shared by all requests.
 * ============================================================
 */

proxy.on(
  'proxyRes',
  (
    proxyRes,
    req,
  ) => {
    if (mode !== 'record') {
      return;
    }

    const requestInfo =
      requestsToRecord.get(req);

    if (!requestInfo) {
      return;
    }

    const contentType =
      String(
        proxyRes.headers[
          'content-type'
        ] ?? '',
      );

    if (
      !shouldRecordResponse(
        requestInfo.url,
        contentType,
      )
    ) {
      requestsToRecord.delete(req);

      return;
    }

    const chunks: Buffer[] = [];

    proxyRes.on(
      'data',
      chunk => {
        chunks.push(
          Buffer.isBuffer(chunk)
            ? chunk
            : Buffer.from(chunk),
        );
      },
    );

    proxyRes.on(
      'end',
      () => {
        try {
          const responseBody =
            Buffer.concat(chunks);

          saveHttpRecording(
            requestInfo,
            proxyRes,
            responseBody,
          );
        } finally {
          requestsToRecord.delete(req);
        }
      },
    );

    proxyRes.on(
      'error',
      () => {
        requestsToRecord.delete(req);
      },
    );
  },
);

/*
 * ============================================================
 * SAVE HTTP RECORDING
 *
 * JSON/text:
 *
 *   Buffer -> UTF-8 string
 *
 * Binary:
 *
 *   Buffer -> base64 string
 *
 * JSON is NOT parsed.
 * ============================================================
 */

function saveHttpRecording(
  requestInfo: {
    body: Buffer;
    url: string;
    method: string;
  },
  proxyRes: http.IncomingMessage,
  responseBody: Buffer,
) {
  const contentType =
    String(
      proxyRes.headers[
        'content-type'
      ] ?? '',
    ).toLowerCase();

  const isJson =
    contentType.includes(
      'application/json',
    ) ||
    contentType.includes(
      '+json',
    );

  let body: string;
  let bodyEncoding:
    | 'utf8'
    | 'base64';

  if (isJson) {
    /*
     * IMPORTANT:
     *
     * We keep JSON as a UTF-8 string.
     *
     * No JSON.parse().
     */
    body =
      responseBody.toString(
        'utf8',
      );

    bodyEncoding = 'utf8';
  } else {
    body =
      responseBody.toString(
        'base64',
      );

    bodyEncoding = 'base64';
  }

  const requestBody =
    requestInfo.body.toString(
      'utf8',
    );

  const key =
    makeRequestKey(
      requestInfo.method,
      requestInfo.url,
      requestBody,
    );

  const recording:
    HttpRecording = {
      key,

      method:
        requestInfo.method,

      url:
        requestInfo.url,

      requestBody,

      response: {
        status:
          proxyRes.statusCode ?? 500,

        headers:
          headersToObject(
            proxyRes.headers,
          ),

        body,

        bodyEncoding,
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
    'utf8',
  );

  console.log(
    `[HTTP RECORD] ${requestInfo.method} ${requestInfo.url}`,
  );
}

/*
 * ============================================================
 * PROXY TO TARGET
 * ============================================================
 */

function proxyToTarget(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  bodyBuffer: Buffer,
) {
  proxy.web(
    req,
    res,
    {
      target: TARGET,

      /*
       * The request body has already been consumed by
       * readBody(), therefore it has to be supplied again.
       */
      buffer:
        Readable.from(
          bodyBuffer,
        ),
    },
  );
}

/*
 * ============================================================
 * RECORD HTTP
 * ============================================================
 */

async function recordHttp(
  req: http.IncomingMessage,
  res: http.ServerResponse,
) {
  const body =
    await readBody(req);

  const url =
    req.url ?? '/';

  const method =
    req.method ?? 'GET';

  /*
   * Only requests matching include/exclude are put
   * into the recording map.
   *
   * proxyRes will later use the same req object.
   */
  if (
    isInRecordScope(url)
  ) {
    requestsToRecord.set(
      req,
      {
        body,
        url,
        method,
      },
    );
  }

  console.log(
    `[HTTP → TARGET] ${method} ${url}`,
  );

  proxyToTarget(
    req,
    res,
    body,
  );
}

/*
 * ============================================================
 * REPLAY HTTP
 *
 * Logic:
 *
 * 1. URL not in include -> proxy
 * 2. URL in include -> look for recording
 * 3. recording found -> replay
 * 4. recording missing -> fallback to target
 * ============================================================
 */

async function replayHttp(
  req: http.IncomingMessage,
  res: http.ServerResponse,
) {
  const body =
    await readBody(req);

  const method =
    req.method ?? 'GET';

  const url =
    req.url ?? '/';

  /*
   * Not included:
   * always proxy to target.
   */
  if (
    !isInRecordScope(url)
  ) {
    console.log(
      `[HTTP → TARGET] ${method} ${url}`,
    );

    proxyToTarget(
      req,
      res,
      body,
    );

    return;
  }

  const requestBody =
    body.toString('utf8');

  const key =
    makeRequestKey(
      method,
      url,
      requestBody,
    );

  const filename =
    path.join(
      httpDir,
      `${key}.json`,
    );

  /*
   * Included but no saved recording:
   * fallback to target.
   */
  if (
    !fs.existsSync(filename)
  ) {
    console.log(
      `[HTTP MISS → TARGET] ${method} ${url}`,
    );

    proxyToTarget(
      req,
      res,
      body,
    );

    return;
  }

  const recording:
    HttpRecording =
    JSON.parse(
      fs.readFileSync(
        filename,
        'utf8',
      ),
    );

  let responseBody: Buffer;

  if (
    recording.response.bodyEncoding ===
    'utf8'
  ) {
    /*
     * JSON/text was stored as UTF-8 string.
     *
     * Restore exactly the same bytes.
     */
    responseBody =
      Buffer.from(
        recording.response.body,
        'utf8',
      );
  } else {
    /*
     * Binary was stored as base64.
     */
    responseBody =
      Buffer.from(
        recording.response.body,
        'base64',
      );
  }

  const headers = {
    ...recording.response.headers,
  };

  /*
   * These headers belong to the original transfer and
   * should not be replayed directly.
   */
  delete headers.connection;
  delete headers['transfer-encoding'];
  delete headers['content-length'];

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

  res.end(responseBody);
}

/*
 * ============================================================
 * WEBSOCKET REPLAY
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
      data => {
        console.log(
          '[WS CLIENT → PROXY]',
          data.toString(),
        );
      },
    );
  },
);

/*
 * ============================================================
 * WEBSOCKET RECORD
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

function saveWsMessage(
  url: string,
  data: WebSocket.RawData,
  binary: boolean,
) {
  const createdAt =
    new Date().toISOString();

  const id =
    makeWsId(createdAt);

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

  fs.writeFileSync(
    path.join(
      wsDir,
      `${id}.json`,
    ),
    JSON.stringify(
      recording,
      null,
      2,
    ),
    'utf8',
  );

  console.log(
    `[WS RECORD] ${id}`,
  );
}

function recordWebSocket(
  req: http.IncomingMessage,
  browserWs: WebSocket,
) {
  const targetUrl =
    new URL(TARGET);

  const protocol =
    targetUrl.protocol === 'https:'
      ? 'wss:'
      : 'ws:';

  const wsUrl =
    `${protocol}//${targetUrl.host}${req.url ?? '/'}`;

  const targetWs =
    new WebSocket(
      wsUrl,
      {
        rejectUnauthorized,
      },
    );

  targetWs.on(
    'open',
    () => {
      console.log(
        `[WS RECORD] connected ${wsUrl}`,
      );

      browserWs.on(
        'message',
        (
          data,
          isBinary,
        ) => {
          if (
            targetWs.readyState ===
            WebSocket.OPEN
          ) {
            targetWs.send(
              data,
              {
                binary: isBinary,
              },
            );
          }
        },
      );

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
}

/*
 * ============================================================
 * WS LIST
 * ============================================================
 */

function getWsRecordings():
  WsRecording[] {
  if (!fs.existsSync(wsDir)) {
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
      `ID:       ${recording.id}`,
    );

    console.log(
      `URL:      ${recording.url}`,
    );

    console.log(
      `Binary:   ${recording.binary}`,
    );

    console.log(
      `Created:  ${recording.createdAt}`,
    );

    console.log(
      'Message:',
    );

    console.log(
      recording.binary
        ? '<binary>'
        : recording.data,
    );
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
  if (!fs.existsSync(httpDir)) {
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

  if (files.length === 0) {
    console.log(
      'No REST recordings found.',
    );

    return;
  }

  for (
    const file
    of files
  ) {
    const recording:
      HttpRecording =
      JSON.parse(
        fs.readFileSync(
          path.join(
            httpDir,
            file,
          ),
          'utf8',
        ),
      );

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
      `Encoding:     ${recording.response.bodyEncoding}`,
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

    /*
     * Do not console.dir here:
     * body is deliberately a string.
     */
    console.log(
      recording.response.body,
    );
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
  const response =
    await fetch(
      `http://127.0.0.1:${PORT}` +
      `/__mock/ws/send/` +
      encodeURIComponent(id),
      {
        method: 'POST',
      },
    );

  const text =
    await response.text();

  console.log(text);

  if (!response.ok) {
    process.exit(1);
  }
}

/*
 * ============================================================
 * SERVER
 * ============================================================
 */

const server =
  http.createServer(
    async (
      req,
      res,
    ) => {
      try {
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
              target: TARGET,
              wsClients:
                replayClients.size,
            },
          );

          return;
        }

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
            !fs.existsSync(filename)
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

          const data =
            Buffer.from(
              recording.data,
              recording.dataEncoding,
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

        if (mode === 'record') {
          await recordHttp(
            req,
            res,
          );
        } else {
          await replayHttp(
            req,
            res,
          );
        }
      } catch (error) {
        console.error(
          '[SERVER ERROR]',
          error,
        );

        if (!res.headersSent) {
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
 */

server.on(
  'upgrade',
  (
    req,
    socket,
    head,
  ) => {
    if (
      req.url?.startsWith(
        '/__mock/',
      )
    ) {
      socket.destroy();

      return;
    }

    if (mode === 'record') {
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
 * JSON RESPONSE
 * ============================================================
 */

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