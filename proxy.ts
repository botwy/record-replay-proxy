import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Readable } from 'node:stream';

import httpProxy from 'http-proxy';
import WebSocket, {
  WebSocketServer,
} from 'ws';
import picomatch from 'picomatch';

type Config = {
  target: string;
  port: number;

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
  };
};

type WsRecording = {
  id: string;
  url: string;
  binary: boolean;
  data: string;
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
  'config.json'
);

const config: Config = JSON.parse(
  fs.readFileSync(
    configPath,
    'utf8'
  )
);

const command =
  process.argv[2] ?? 'replay';

const validCommands = [
  'record',
  'replay',
  'ws:list',
  'ws:send',
];

if (!validCommands.includes(command)) {
  console.error(`
Usage:

  npm run record
  npm run replay

  npm run ws:list
  npm run ws:send -- <id>
`);

  process.exit(1);
}

const mode =
  command === 'record'
    ? 'record'
    : 'replay';

const PORT = config.port;
const TARGET = config.target;

/*
 * ============================================================
 * RECORDINGS
 * ============================================================
 */

const recordingsDir =
  path.join(
    __dirname,
    'recordings'
  );

const httpDir =
  path.join(
    recordingsDir,
    'http'
  );

const wsDir =
  path.join(
    recordingsDir,
    'ws'
  );

fs.mkdirSync(
  httpDir,
  { recursive: true }
);

fs.mkdirSync(
  wsDir,
  { recursive: true }
);

/*
 * ============================================================
 * GLOB MATCHERS
 * ============================================================
 */

const includeMatcher =
  picomatch(
    config.record.include
  );

const excludeMatcher =
  picomatch(
    config.record.exclude ?? []
  );

function shouldRecordHttp(
  requestUrl: string,
  contentType: string
): boolean {
  /*
   * requestUrl может быть как:
   *
   * /api/users
   *
   * так и:
   *
   * https://example.com/api/users
   *
   * Нас интересует только pathname.
   */

  const url = new URL(
    requestUrl,
    TARGET
  );

  const pathname =
    url.pathname;

  if (
    !includeMatcher(pathname)
  ) {
    return false;
  }

  if (
    excludeMatcher(pathname)
  ) {
    return false;
  }

  const normalizedContentType =
    contentType.toLowerCase();

  return config.record.contentTypes.some(
    type =>
      normalizedContentType.includes(
        type.toLowerCase()
      )
  );
}

/*
 * ============================================================
 * HELPERS
 * ============================================================
 */

function sha256(
  value: string
): string {
  return crypto
    .createHash('sha256')
    .update(value)
    .digest('hex');
}

function makeRequestKey(
  method: string,
  url: string,
  body: string
): string {
  return sha256(
    [
      method.toUpperCase(),
      url,
      body,
    ].join('\n')
  );
}

function readBody(
  req: http.IncomingMessage
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
              : Buffer.from(chunk)
          );
        }
      );

      req.on(
        'end',
        () => {
          resolve(
            Buffer.concat(chunks)
          );
        }
      );

      req.on(
        'error',
        reject
      );
    }
  );
}

function headersToObject(
  headers: http.IncomingHttpHeaders
) {
  const result: Record<
    string,
    string
  > = {};

  for (
    const [key, value]
    of Object.entries(headers)
  ) {
    if (
      value === undefined
    ) {
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
  data: unknown
) {
  const body =
    JSON.stringify(
      data,
      null,
      2
    );

  res.writeHead(
    status,
    {
      'content-type':
        'application/json; charset=utf-8',

      'content-length':
        String(
          Buffer.byteLength(body)
        ),
    }
  );

  res.end(body);
}

/*
 * ============================================================
 * HTTP PROXY
 * ============================================================
 */

const proxy =
  httpProxy.createProxyServer({
    changeOrigin: true,
    secure: true,
  });

proxy.on(
  'error',
  (error, _req, res) => {
    console.error(
      '[PROXY ERROR]',
      error.message
    );

    if (
      res &&
      !res.headersSent
    ) {
      res.writeHead(502);
      res.end(
        'Proxy error'
      );
    }
  }
);

/*
 * ============================================================
 * HTTP RECORD
 * ============================================================
 */

async function recordHttp(
  req: http.IncomingMessage,
  res: http.ServerResponse
) {
  const bodyBuffer =
    await readBody(req);

  const body =
    bodyBuffer.toString('utf8');

  const method =
    req.method ?? 'GET';

  const url =
    req.url ?? '/';

  proxy.once(
    'proxyRes',
    proxyRes => {
      const contentType =
        String(
          proxyRes.headers[
            'content-type'
          ] ?? ''
        );

      if (
        !shouldRecordHttp(
          url,
          contentType
        )
      ) {
        return;
      }

      const chunks: Buffer[] = [];

      proxyRes.on(
        'data',
        chunk => {
          chunks.push(
            Buffer.isBuffer(chunk)
              ? chunk
              : Buffer.from(chunk)
          );
        }
      );

      proxyRes.on(
        'end',
        () => {
          const responseBody =
            Buffer.concat(chunks);

          const key =
            makeRequestKey(
              method,
              url,
              body
            );

          const recording:
            HttpRecording = {
              key,
              method,
              url,
              requestBody:
                body,

              response: {
                status:
                  proxyRes.statusCode ??
                  500,

                headers:
                  headersToObject(
                    proxyRes.headers
                  ),

                body:
                  responseBody.toString(
                    'base64'
                  ),
              },
            };

          const filename =
            path.join(
              httpDir,
              `${key}.json`
            );

          fs.writeFileSync(
            filename,
            JSON.stringify(
              recording,
              null,
              2
            )
          );

          console.log(
            `[HTTP RECORD] ${method} ${url}`
          );
        }
      );
    }
  );

  proxy.web(
    req,
    res,
    {
      target: TARGET,

      buffer:
        Readable.from(
          bodyBuffer
        ),
    }
  );
}

/*
 * ============================================================
 * HTTP REPLAY
 * ============================================================
 */

async function replayHttp(
  req: http.IncomingMessage,
  res: http.ServerResponse
) {
  const bodyBuffer =
    await readBody(req);

  const body =
    bodyBuffer.toString('utf8');

  const method =
    req.method ?? 'GET';

  const url =
    req.url ?? '/';

  const key =
    makeRequestKey(
      method,
      url,
      body
    );

  const filename =
    path.join(
      httpDir,
      `${key}.json`
    );

  if (
    !fs.existsSync(filename)
  ) {
    console.warn(
      `[HTTP MISS] ${method} ${url}`
    );

    sendJson(
      res,
      404,
      {
        error:
          'No recording found',

        method,
        url,
        key,
      }
    );

    return;
  }

  const recording:
    HttpRecording =
    JSON.parse(
      fs.readFileSync(
        filename,
        'utf8'
      )
    );

  const responseBody =
    Buffer.from(
      recording.response.body,
      'base64'
    );

  const headers = {
    ...recording.response.headers,
  };

  delete headers.connection;
  delete headers['transfer-encoding'];

  headers['content-length'] =
    String(
      responseBody.length
    );

  console.log(
    `[HTTP REPLAY] ${method} ${url}`
  );

  res.writeHead(
    recording.response.status,
    headers
  );

  res.end(
    responseBody
  );
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
      '[WS] client connected'
    );

    replayClients.add(ws);

    ws.on(
      'close',
      () => {
        replayClients.delete(ws);

        console.log(
          '[WS] client disconnected'
        );
      }
    );

    ws.on(
      'error',
      () => {
        replayClients.delete(ws);
      }
    );

    /*
     * В replay режиме сообщения
     * browser → server не воспроизводятся.
     */

    ws.on(
      'message',
      data => {
        console.log(
          '[WS CLIENT → PROXY]',
          data.toString()
        );
      }
    );
  }
);

/*
 * ============================================================
 * WEBSOCKET RECORD
 * ============================================================
 */

function recordWebSocket(
  req: http.IncomingMessage,
  browserWs: WebSocket
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
    const [key, value]
    of Object.entries(
      req.headers
    )
  ) {
    if (
      value === undefined
    ) {
      continue;
    }

    /*
     * Эти headers ws формирует самостоятельно.
     */

    if (
      key === 'host' ||
      key === 'connection' ||
      key === 'upgrade' ||
      key === 'sec-websocket-key' ||
      key === 'sec-websocket-version' ||
      key === 'sec-websocket-extensions'
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
              value.trim()
          )
      : undefined;

  const targetWs =
    new WebSocket(
      wsUrl,
      protocols,
      {
        headers,

        rejectUnauthorized:
          process.env
            .TARGET_TLS_VERIFY !==
          'false',
      }
    );

  targetWs.on(
    'open',
    () => {
      console.log(
        `[WS RECORD] connected ${wsUrl}`
      );

      /*
       * Browser → real server
       */

      browserWs.on(
        'message',
        (
          data,
          isBinary
        ) => {
          if (
            targetWs.readyState ===
            WebSocket.OPEN
          ) {
            targetWs.send(
              data,
              {
                binary:
                  isBinary,
              }
            );
          }
        }
      );

      /*
       * Real server → browser.
       *
       * Эти сообщения сохраняем.
       */

      targetWs.on(
        'message',
        (
          data,
          isBinary
        ) => {
          if (
            browserWs.readyState ===
            WebSocket.OPEN
          ) {
            browserWs.send(
              data,
              {
                binary:
                  isBinary,
              }
            );
          }

          saveWsMessage(
            req.url ?? '/',
            data,
            isBinary
          );
        }
      );
    }
  );

  targetWs.on(
    'error',
    error => {
      console.error(
        '[WS TARGET ERROR]',
        error.message
      );

      if (
        browserWs.readyState ===
        WebSocket.OPEN
      ) {
        browserWs.close(
          1011,
          'Target WebSocket error'
        );
      }
    }
  );

  targetWs.on(
    'close',
    (
      code,
      reason
    ) => {
      console.log(
        `[WS TARGET CLOSE] ${code}`
      );

      if (
        browserWs.readyState ===
        WebSocket.OPEN
      ) {
        browserWs.close(
          code,
          reason
        );
      }
    }
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
    }
  );

  browserWs.on(
    'error',
    () => {
      targetWs.close();
    }
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
  binary: boolean
) {
  const id =
    `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;

  const buffer =
    Buffer.isBuffer(data)
      ? data
      : Buffer.from(
          data as ArrayBuffer
        );

  const recording:
    WsRecording = {
      id,
      url,
      binary,
      data:
        buffer.toString(
          'base64'
        ),
      createdAt:
        new Date().toISOString(),
    };

  fs.writeFileSync(
    path.join(
      wsDir,
      `${id}.json`
    ),
    JSON.stringify(
      recording,
      null,
      2
    )
  );

  console.log(
    `[WS RECORD] ${id}`
  );
}

/*
 * ============================================================
 * WS CLI
 * ============================================================
 */

function getWsRecordings():
  WsRecording[] {
  return fs
    .readdirSync(wsDir)
    .filter(
      file =>
        file.endsWith('.json')
    )
    .sort()
    .map(
      file =>
        JSON.parse(
          fs.readFileSync(
            path.join(
              wsDir,
              file
            ),
            'utf8'
          )
        )
    );
}

function printWsList() {
  const recordings =
    getWsRecordings();

  if (
    recordings.length === 0
  ) {
    console.log(
      'No WebSocket recordings found.'
    );

    return;
  }

  for (
    const recording
    of recordings
  ) {
    let preview: string;

    if (
      recording.binary
    ) {
      preview =
        '<binary>';
    } else {
      preview =
        Buffer.from(
          recording.data,
          'base64'
        ).toString(
          'utf8'
        );

      if (
        preview.length > 200
      ) {
        preview =
          preview.slice(
            0,
            200
          ) + '...';
      }
    }

    console.log('');
    console.log(
      `ID:      ${recording.id}`
    );
    console.log(
      `URL:     ${recording.url}`
    );
    console.log(
      `Binary:  ${recording.binary}`
    );
    console.log(
      `Created: ${recording.createdAt}`
    );
    console.log(
      `Message: ${preview}`
    );
  }

  console.log('');
  console.log(
    `Total: ${recordings.length}`
  );
}

async function sendWsCommand(
  id: string
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
        }
      );

    const body =
      await response.text();

    console.log(body);

    if (
      !response.ok
    ) {
      process.exit(1);
    }
  } catch (error) {
    console.error(
      'Cannot connect to replay proxy.'
    );

    console.error(
      String(error)
    );

    process.exit(1);
  }
}

/*
 * ============================================================
 * CLI COMMANDS
 * ============================================================
 *
 * ws:list и ws:send выполняются отдельным процессом.
 */

if (
  command === 'ws:list'
) {
  printWsList();
  process.exit(0);
}

if (
  command === 'ws:send'
) {
  const id =
    process.argv[3];

  if (!id) {
    console.error(
      'Usage: npm run ws:send -- <id>'
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
      res
    ) => {
      try {
        /*
         * Health endpoint.
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
              target: TARGET,
              wsClients:
                replayClients.size,
            }
          );

          return;
        }

        /*
         * WS recordings list API.
         */

        if (
          req.method === 'GET' &&
          req.url ===
            '/__mock/ws'
        ) {
          sendJson(
            res,
            200,
            getWsRecordings()
          );

          return;
        }

        /*
         * Send WS recording to all connected
         * replay clients.
         */

        const sendMatch =
          req.url?.match(
            /^\/__mock\/ws\/send\/(.+)$/
          );

        if (
          req.method === 'POST' &&
          sendMatch
        ) {
          const id =
            decodeURIComponent(
              sendMatch[1]
            );

          const filename =
            path.join(
              wsDir,
              `${id}.json`
            );

          if (
            !fs.existsSync(
              filename
            )
          ) {
            sendJson(
              res,
              404,
              {
                error:
                  'WS recording not found',
                id,
              }
            );

            return;
          }

          const recording:
            WsRecording =
            JSON.parse(
              fs.readFileSync(
                filename,
                'utf8'
              )
            );

          const data =
            Buffer.from(
              recording.data,
              'base64'
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
              }
            );

            sent++;
          }

          console.log(
            `[WS SEND] ${id} → ${sent} client(s)`
          );

          sendJson(
            res,
            200,
            {
              ok: true,
              id,
              sent,
            }
          );

          return;
        }

        /*
         * REST.
         */

        if (
          mode === 'record'
        ) {
          await recordHttp(
            req,
            res
          );
        } else {
          await replayHttp(
            req,
            res
          );
        }
      } catch (
        error
      ) {
        console.error(
          '[SERVER ERROR]',
          error
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
            }
          );
        }
      }
    }
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
    head
  ) => {
    /*
     * Admin HTTP endpoints не являются WebSocket.
     */

    if (
      req.url?.startsWith(
        '/__mock/'
      )
    ) {
      socket.destroy();
      return;
    }

    /*
     * RECORD:
     *
     * browser → local proxy → real stand
     */

    if (
      mode === 'record'
    ) {
      const wss =
        new WebSocketServer({
          noServer: true,
        });

      wss.handleUpgrade(
        req,
        socket,
        head,
        browserWs => {
          recordWebSocket(
            req,
            browserWs
          );
        }
      );

      return;
    }

    /*
     * REPLAY:
     *
     * browser → local proxy
     *
     * Никакого соединения со стендом.
     */

    replayWss.handleUpgrade(
      req,
      socket,
      head,
      ws => {
        replayWss.emit(
          'connection',
          ws,
          req
        );
      }
    );
  }
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
      '========================================'
    );
    console.log(
      ' REST / WebSocket Record Replay Proxy'
    );
    console.log(
      '========================================'
    );
    console.log(
      `Mode:   ${mode}`
    );
    console.log(
      `Target: ${TARGET}`
    );
    console.log(
      `Port:   ${PORT}`
    );
    console.log(
      '========================================'
    );
    console.log('');
  }
);
