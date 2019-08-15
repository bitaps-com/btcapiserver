from aiohttp.web import middleware

@middleware
async def allow_origin(request, handler):
    resp = await handler(request)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    if 'content_type' not in resp.headers:
        resp.headers['content_type'] = 'application/json'
    return
