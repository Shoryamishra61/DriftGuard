"""Shared network-address policy for outbound HTTP delivery."""

from __future__ import annotations

from ipaddress import IPv4Address, IPv6Address


def is_public_unicast_address(address: IPv4Address | IPv6Address) -> bool:
    """Return whether an address is suitable for a public TCP connection."""

    return (
        address.is_global
        and not address.is_multicast
        and not address.is_reserved
        and not address.is_unspecified
        and not address.is_loopback
        and not address.is_link_local
        and not getattr(address, "is_site_local", False)
    )
