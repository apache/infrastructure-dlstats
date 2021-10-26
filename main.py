#!/usr/bin/env python3

import ahapi
import asyncio

httpserver = ahapi.simple(
    api_dir="/opt/dlstats/scripts",    # serve api end points from here
    static_dir="/opt/dlstats/htdocs",  # serve stuff like html and images from here
    bind_ip="127.0.0.1",
    bind_port=8082,
    state={"something": "stateful"}
)

loop = asyncio.get_event_loop()
loop.run_until_complete(httpserver.loop())
