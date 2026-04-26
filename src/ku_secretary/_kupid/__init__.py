"""Vendored Korea University portal/LMS client modules.

Source: https://github.com/SonAIengine/ku-portal-mcp (v0.10.1, MIT License)
Author: SonAIengine <sonsj97@korea.ac.kr>

The MCP server entry points (server.py, __main__.py) are intentionally not
vendored. KU Secretary uses these modules as a library — the FastMCP runtime
and its dependencies are not pulled in.

Modules used:
- auth         KUPID SSO login + session caching
- library      Library seat availability (no auth)
- timetable    Personal class timetable + ICS export
- scraper      Notice/schedule/scholarship board parsing
- courses      Course search + syllabus + my-courses
- grades       All-grade lookup (cumulative GPA, earned credits)
- lms          Canvas LMS via KSSO SAML SSO
- dept_notices Department notice scraping
- dept_registry / academic  helpers

See LICENSE in this directory for the upstream MIT license terms.
"""

__upstream_version__ = "0.10.1"
__upstream_source__ = "https://github.com/SonAIengine/ku-portal-mcp"
