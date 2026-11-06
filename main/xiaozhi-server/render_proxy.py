import os
import asyncio
from aiohttp import web, ClientSession, WSMsgType


HTTP_BACKEND = "http://127.0.0.1:8003"
WS_BACKEND = "ws://127.0.0.1:8000"

HOP_BY_HOP_HEADERS = {
    "connection",
    "upgrade",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
}


async def websocket_proxy(request, session):
    ws_client = web.WebSocketResponse(autoping=True)
    await ws_client.prepare(request)

    ws_url = WS_BACKEND + str(request.rel_url)

    try:
        async with session.ws_connect(
            ws_url,
            protocols=request.headers.getall(
                "Sec-WebSocket-Protocol", []
            ),
            origin=request.headers.get("Origin"),
            heartbeat=30,
        ) as ws_server:

            async def client_to_server():
                async for msg in ws_client:
                    if msg.type == WSMsgType.TEXT:
                        await ws_server.send_str(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        await ws_server.send_bytes(msg.data)
                    elif msg.type == WSMsgType.CLOSE:
                        await ws_server.close()
                        break

            async def server_to_client():
                async for msg in ws_server:
                    if msg.type == WSMsgType.TEXT:
                        await ws_client.send_str(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        await ws_client.send_bytes(msg.data)
                    elif msg.type == WSMsgType.CLOSE:
                        await ws_client.close()
                        break
                    elif msg.type == WSMsgType.ERROR:
                        break

            tasks = [
                asyncio.create_task(client_to_server()),
                asyncio.create_task(server_to_client()),
            ]

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            await asyncio.gather(*pending, return_exceptions=True)

    except Exception as error:
        print(f"Error en proxy WebSocket: {error}")
        if not ws_client.closed:
            await ws_client.close()

    return ws_client


async def proxy_handler(request):
    session = request.app["session"]

    if request.path.startswith("/xiaozhi/v1"):
        return await websocket_proxy(request, session)

    target_url = HTTP_BACKEND + str(request.rel_url)

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
        and key.lower() != "host"
    }

    try:
        async with session.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.content,
            allow_redirects=False,
        ) as response:

            response_headers = {
                key: value
                for key, value in response.headers.items()
                if key.lower() not in HOP_BY_HOP_HEADERS
            }

            proxy_response = web.StreamResponse(
                status=response.status,
                headers=response_headers,
            )

            await proxy_response.prepare(request)

            async for chunk in response.content.iter_chunked(65536):
                await proxy_response.write(chunk)

            await proxy_response.write_eof()
            return proxy_response

    except Exception as error:
        print(f"Error en proxy HTTP: {error}")
        return web.Response(
            status=502,
            text="Error conectando con el servidor interno",
        )


async def start_proxy():
    app = web.Application()
    app["session"] = ClientSession()

    app.router.add_route("*", "/{tail:.*}", proxy_handler)

    async def cleanup(app):
        await app["session"].close()

    app.on_cleanup.append(cleanup)

    port = int(os.environ.get("PORT", 10000))

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=port,
    )

    await site.start()

    print(f"Proxy de Render escuchando en el puerto {port}")

    return runner
