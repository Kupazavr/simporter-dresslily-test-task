import configparser


def parse_config(part):
    config = configparser.ConfigParser()
    config.read('../config.ini')
    if part != 'ALL':
        config = config[part]
    return config


def chunkify(l, n):
    """Chunking list in n parts"""
    for i in range(0, len(l), n):
        yield l[i:i + n]



