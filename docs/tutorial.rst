Tutorial
========
Making a rate limit middleware
------------------------------

Lets make a simple FastAPI app and rate limit it in various ways.
To start lets make a simple FastAPI app with a dummy middleware and a few dummy endpoints

.. code:: python3

    from fastapi import FastAPI, Request, Response

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint() -> str:
        return "test ok"

    @app.get("/example")
    async def example_endpoint() -> str:
        return "examples!"

    @app.get("/error")
    async def error_endpoint() -> None:
        assert 1 == 2

    @app.middleware("http")
    async def apply_rate_limits(request: Request, call_next: typing.Callable[[Request], typing.Awaitable[Response]]) -> Response:
        return await call_next(request)

This works great and all, but it's missing rate limits. Lets add a rate limit of 10 requests every 10 seconds per IP.

.. code:: python3

    from collections import defaultdict
    from slowstack.common.errors import RateLimitedError
    from slowstack.asynchronous.times_per import TimesPerRateLimiter
    
    global_rate_limiters: defaultdict[str, TimesPerRateLimiter] = defaultdict(lambda: TimesPerRateLimiter(10, 10)) # 10 requests per 10 seconds per ip

    @app.middleware("http")
    async def apply_rate_limits(request: Request, call_next: typing.Callable[[Request], typing.Awaitable[Response]]) -> Response:
        if request.client:
            ip = request.client.host
        else:
            ip = "127.0.0.1"

        global_rate_limiter = global_rate_limiters[ip]
        
        try:
            async with global_rate_limiter.acquire(wait=False):
                return await call_next(request)
        except RateLimitedError:
            return JSONResponse({"detail": "rate limited"}, status_code=429)

So now we have global rate limits which is great and all, but what if we wanted per-endpoint rate limits?

.. code:: python3

    route_rate_limiters: defaultdict[str, defaultdict[str, TimesPerRateLimiter]] = defaultdict(lambda: defaultdict(lambda: TimesPerRateLimiter(1, 1))) # 1 request per second per endpoint per ip

    @app.middleware("http")
    async def apply_rate_limits(request: Request, call_next: typing.Callable[[Request], typing.Awaitable[Response]]) -> Response:
        ...

        endpoint = request.scope["root_path"] + request.scope["path"]
        route_rate_limiter = route_rate_limiters[ip][endpoint]

        all_rate_limiters = AllRateLimiter({global_rate_limiter, route_rate_limiter})

        try:
            async with all_rate_limiters.acquire(wait=False):
                return await call_next(request)
        except RateLimitedError:
            return JSONResponse({"detail": "rate limited"}, status_code=429)
.. warning::
    Do not stack rate limiters like below as that will make them slower!

    .. code:: python3
        
        async with global_rate_limiter.acquire():
            async with route_rate_limiter.acquire():
                ...
.. note::
    You can also use :class:`AnyRateLimiter` to use the first rate limiter available

Now since some of our endpoints is a bit error-prone we might not want to count uses of the rate limit when we have an error

.. code:: python3

    from slowstack.common.errors import RateLimitCancelError
    
    @app.middleware("http")
    async def apply_rate_limits(request: Request, call_next: typing.Callable[[Request], typing.Awaitable[Response]]) -> Response:
        ...

        response: Response | None = None
        try:
            async with all_rate_limiter.acquire(wait=False):
                response = await call_next(request)
                if response.status_code == 500:
                    raise RateLimitCancelError()
        except RateLimitedError:
            return JSONResponse({"detail": "rate limited"}, status_code=429)
        except RateLimitCancelError:
            pass
        
        assert response is not None
        return response

Now the full code is

.. code:: python3

    from fastapi.responses import JSONResponse
    from fastapi import FastAPI, Request, Response
    from slowstack.asynchronous.times_per import TimesPerRateLimiter
    from slowstack.asynchronous.all import AllRateLimiter
    from slowstack.common.errors import RateLimitCancelError, RateLimitedError
    from collections import defaultdict
    import typing

    app = FastAPI()

    global_rate_limiters: defaultdict[str, TimesPerRateLimiter] = defaultdict(lambda: TimesPerRateLimiter(10, 10)) # 10 requests per 10 seconds per ip
    route_rate_limiters: defaultdict[str, defaultdict[str, TimesPerRateLimiter]] = defaultdict(lambda: defaultdict(lambda: TimesPerRateLimiter(1, 1))) # 1 request per second per endpoint per ip

    @app.get("/test")
    async def test_endpoint() -> str:
        return "test ok"

    @app.get("/example")
    async def example_endpoint() -> str:
        return "examples!"

    @app.get("/error")
    async def error_endpoint() -> None:
        assert 1 == 2

    @app.middleware("http")
    async def apply_rate_limits(request: Request, call_next: typing.Callable[[Request], typing.Awaitable[Response]]) -> Response:
        if request.client:
            ip = request.client.host
        else:
            ip = "127.0.0.1"

        global_rate_limiter = global_rate_limiters[ip]

        endpoint = request.scope["root_path"] + request.scope["path"]
        route_rate_limiter = route_rate_limiters[ip][endpoint]

        all_rate_limiter = AllRateLimiter({global_rate_limiter, route_rate_limiter})
        
        response: Response | None = None
        try:
            async with all_rate_limiter.acquire(wait=False):
                response = await call_next(request)
                if response.status_code == 500:
                    raise RateLimitCancelError()
        except RateLimitedError:
            return JSONResponse({"detail": "rate limited"}, status_code=429)
        except RateLimitCancelError:
            pass
        
        assert response is not None
        return response


Implementing the client for our server
--------------------------------------
Lets say we have a client for the API we made in the previous example

.. code:: python3

    import asyncio
    from collections import defaultdict
    import random
    from aiohttp import ClientSession

    class TestClient:
        def __init__(self) -> None:
            self._session: ClientSession | None = None

        async def _get_session(self) -> ClientSession:
            if self._session is None:
                self._session = ClientSession(base_url="http://localhost:8000")
            return self._session

        async def do_request(self, path: str) -> int:
            session = await self._get_session()

            response = await session.get(path)
            response.close()
            return response.status

        async def test(self) -> None:
            paths = ["/test", "/example", "/error"]
            statuses = await asyncio.gather(*[self.do_request(random.choice(paths)) for _ in range(100)])

            status_to_count = defaultdict(lambda: 0)

            for status in statuses:
                status_to_count[status] += 1
            
            for status, count in status_to_count.items():
                print(f"{status} -> {count}")
            session = await self._get_session()

            # Clean up
            await session.close()
            self._session = None

    test_client = TestClient()
    asyncio.run(test_client.test())

Lets add some rate limiters to it!

.. code:: python3

    import asyncio
    from collections import defaultdict
    import random
    from aiohttp import ClientSession
    from slowstack.asynchronous.base import BaseRateLimiter
    from slowstack.asynchronous.all import AllRateLimiter
    from slowstack.asynchronous.times_per import TimesPerRateLimiter 

    class TestClient:
        def __init__(self) -> None:
            self._session: ClientSession | None = None
            self._global_rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(10, 10)
            self._endpoint_rate_limiters: defaultdict[str, TimesPerRateLimiter] = defaultdict(lambda: TimesPerRateLimiter(1, 1))

        async def _get_session(self) -> ClientSession:
            if self._session is None:
                self._session = ClientSession(base_url="http://localhost:8000")
            return self._session
        
        def _get_rate_limiter(self, path: str) -> BaseRateLimiter:
            return AllRateLimiter({
                self._global_rate_limiter,
                self._endpoint_rate_limiters[path]
            })
        async def do_request(self, path: str) -> int:
            session = await self._get_session()

            async with self._get_rate_limiter(path).acquire():
                response = await session.get(path)
            response.close()
            return response.status

        async def test(self) -> None:
            paths = ["/test", "/example", "/error"]
            statuses = await asyncio.gather(*[self.do_request(random.choice(paths)) for _ in range(100)])

            status_to_count = defaultdict(lambda: 0)

            for status in statuses:
                status_to_count[status] += 1
            
            for status, count in status_to_count.items():
                print(f"{status} -> {count}")
            session = await self._get_session()

            # Clean up
            await session.close()
            self._session = None

    test_client = TestClient()
    asyncio.run(test_client.test())

We can also add back the 500 server error reverting

.. code:: python3

    import asyncio
    from collections import defaultdict
    import random
    from aiohttp import ClientSession, ClientResponse
    from slowstack.asynchronous.base import BaseRateLimiter
    from slowstack.asynchronous.all import AllRateLimiter
    from slowstack.asynchronous.times_per import TimesPerRateLimiter 

    import logging

    from slowstack.common.errors import RateLimitCancelError
    logging.basicConfig(level=logging.DEBUG)

    class TestClient:
        def __init__(self) -> None:
            self._session: ClientSession | None = None
            self._global_rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(10, 10)
            self._endpoint_rate_limiters: defaultdict[str, TimesPerRateLimiter] = defaultdict(lambda: TimesPerRateLimiter(1, 1))

        async def _get_session(self) -> ClientSession:
            if self._session is None:
                self._session = ClientSession(base_url="http://localhost:8000")
            return self._session
        
        def _get_rate_limiter(self, path: str) -> BaseRateLimiter:
            return AllRateLimiter({
                self._global_rate_limiter,
                self._endpoint_rate_limiters[path]
            })
        async def do_request(self, path: str) -> int:
            session = await self._get_session()
            
            response: ClientResponse | None = None
            try:
                async with self._get_rate_limiter(path).acquire():
                    response = await session.get(path)

                    if response.status == 500:
                        raise RateLimitCancelError()
            except RateLimitCancelError:
                pass
            assert response is not None
            response.close()
            return response.status

        async def test(self) -> None:
            paths = ["/test", "/example", "/error"]
            statuses = await asyncio.gather(*[self.do_request(random.choice(paths)) for _ in range(20)])

            status_to_count = defaultdict(lambda: 0)

            for status in statuses:
                status_to_count[status] += 1
            
            for status, count in status_to_count.items():
                print(f"{status} -> {count}")
            session = await self._get_session()

            # Clean up
            await session.close()
            self._session = None

    test_client = TestClient()
    asyncio.run(test_client.test())
