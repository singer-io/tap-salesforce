import unittest
import tap_salesforce.chunking as chunking

class Chunking(unittest.TestCase):
    def test_chunk_id_range_small(self):
        start_id = "a00J0000000000z" # 61
        end_id = "a00J00000000011"   # 63
        chunk_size = 10
        chunks = chunking.chunk_id_range(start_id, end_id, chunk_size)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], ("a00J0000000000z", "a00J00000000011"))

    def test_chunk_id_range_medium(self):
        start_id = "a00J0000000000z" # 61
        end_id = "a00J0000000001F"   # 77
        chunk_size = 10
        chunks = chunking.chunk_id_range(start_id, end_id, chunk_size)

        self.assertEqual(len(chunks), 2)

        # 61-70
        self.assertEqual(chunks[0], ("a00J0000000000z", "a00J00000000018"))

        # 71-77
        self.assertEqual(chunks[1], ("a00J00000000019", "a00J0000000001F"))

    def test_chunk_id_range_larger(self):
        start_id = "a00J0000000000z" # 61
        end_id = "a00J0000000001L"   # 83
        chunk_size = 10
        chunks = chunking.chunk_id_range(start_id, end_id, chunk_size)

        self.assertEqual(len(chunks), 3)

        # 61-70
        self.assertEqual(chunks[0], ("a00J0000000000z", "a00J00000000018"))

        # 71-79
        self.assertEqual(chunks[1], ("a00J00000000019", "a00J0000000001I"))

        # 80-83
        self.assertEqual(chunks[2], ("a00J0000000001J", "a00J0000000001L"))

    def test_encoded_chunk_tuple(self):
        prefix = "ABC123"

        encoded_tuple = chunking.encoded_chunk_tuple(prefix, 1, 2)
        self.assertEqual(encoded_tuple, ("ABC123000000001", "ABC123000000002"))

        encoded_tuple = chunking.encoded_chunk_tuple(prefix, 10, 20)
        self.assertEqual(encoded_tuple, ("ABC12300000000A", "ABC12300000000K"))

        encoded_tuple = chunking.encoded_chunk_tuple(prefix, 63, 21000000)
        self.assertEqual(encoded_tuple, ("ABC123000000011", "ABC12300001Q73g"))
