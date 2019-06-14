import io


def path_to_bytes(path):
    return path_to_buffer(path).getvalue()


def path_to_buffer(path):
    with open(path, 'rb') as f:
        data = io.BytesIO(f.read())
    data.seek(0)
    return data
