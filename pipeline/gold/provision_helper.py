import hashlib


def generate_sk(series):
    """Generates a stable 64-bit integer hash from a natural key."""
    return series.apply(lambda x: int(hashlib.sha256(str(x).encode()).hexdigest(), 16) % (2 ** 63))
