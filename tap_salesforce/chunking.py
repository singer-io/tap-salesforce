import base62

ID_FORMAT_STR = "{}{}"

def chunk_id_range(start_id, end_id, chunk_size):
    prefix = start_id[:6]

    start_id_stripped = start_id[6:]
    end_id_stripped = end_id[6:]

    start_num = base62.decode(start_id_stripped)
    end_num = base62.decode(end_id_stripped)

    num_range = end_num - start_num

    chunks = []

    if num_range < chunk_size:
        return [encoded_chunk_tuple(prefix, start_num, end_num)]

    num_chunks = (num_range // chunk_size) - 1

    first_chunk = start_num
    last_chunk = start_num + chunk_size - 1
    chunks.append(encoded_chunk_tuple(prefix, first_chunk, last_chunk))

    for i in range(num_chunks):
        first_chunk = last_chunk + 1
        last_chunk = first_chunk + chunk_size - 1
        chunks.append(encoded_chunk_tuple(prefix, first_chunk, last_chunk))

    if last_chunk < end_num:
        chunks.append(encoded_chunk_tuple(prefix, last_chunk+1, end_num))

    return chunks

def encoded_chunk_tuple(prefix, start_num, end_num):
    encoded_start = base62.encode(start_num)
    encoded_end = base62.encode(end_num)

    while len(encoded_start) < 9:
        encoded_start = '0' + encoded_start

    while len(encoded_end) < 9:
        encoded_end = '0' + encoded_end

    return (
        ID_FORMAT_STR.format(prefix, encoded_start),
        ID_FORMAT_STR.format(prefix, encoded_end))
