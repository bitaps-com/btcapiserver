import ipaddress
import base64



def ip_address_to_bytes(a):
    if a[-6:] == '.onion':
        return  b'\xfd\x87\xd8\x7e\xeb\x43' + base64.b32decode(a[:-6].upper())
    try:
        return ipaddress.IPv6Address(a).packed
    except:
        return ipaddress.IPv6Address('::ffff:' + a).packed

def bytes_to_address(b):
    if b[:6] == b'\xfd\x87\xd8\x7e\xeb\x43':
        return base64.b32encode(b[6:]).decode().lower()+'.onion'
    else:
        try:
            a = ipaddress.IPv6Address(b)
            if a.ipv4_mapped is not None:
                return str(a.ipv4_mapped)
            return str(a)
        except:
            a = ipaddress.IPv4Address(b)
            return str(a)

def network_type(address):
    if address.endswith(".onion"):
        return "TOR"
    try:
        ipaddress.IPv6Address(address)
        return "IPv6"
    except:
        return "IPv4"

