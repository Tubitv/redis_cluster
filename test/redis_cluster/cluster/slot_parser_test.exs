defmodule RedisCluster.Cluster.SlotParserTest do
  use ExUnit.Case, async: true

  alias RedisCluster.Cluster.SlotParser
  alias RedisCluster.Cluster.NodeInfo

  @moduletag :parser

  test "should parse the output of the CLUSTER SLOTS command without fragmented nodes" do
    result =
      basic_data()
      |> SlotParser.parse()
      |> NodeInfo.to_table()
      |> IO.iodata_to_binary()
      |> normalize_table()

    expected =
      String.trim_trailing("""
      Slot Start | Slot End | Host      | Port | Role    | Health
      ---------- | -------- | --------- | ---- | ------- | -------
      0          | 5460     | 127.0.0.1 | 6379 | master  | unknown
      0          | 5460     | 127.0.0.1 | 6383 | replica | unknown
      0          | 5460     | 127.0.0.1 | 6384 | replica | unknown
      0          | 5460     | 127.0.0.1 | 6389 | replica | unknown
      5461       | 10922    | 127.0.0.1 | 6380 | master  | unknown
      5461       | 10922    | 127.0.0.1 | 6382 | replica | unknown
      5461       | 10922    | 127.0.0.1 | 6385 | replica | unknown
      5461       | 10922    | 127.0.0.1 | 6386 | replica | unknown
      10923      | 16383    | 127.0.0.1 | 6381 | master  | unknown
      10923      | 16383    | 127.0.0.1 | 6387 | replica | unknown
      10923      | 16383    | 127.0.0.1 | 6388 | replica | unknown
      10923      | 16383    | 127.0.0.1 | 6390 | replica | unknown
      """)

    assert result == expected
  end

  test "should parse the output of the CLUSTER SLOTS command with fragmented nodes" do
    result =
      fragmented_data()
      |> SlotParser.parse()
      |> NodeInfo.to_table()
      |> IO.iodata_to_binary()
      |> normalize_table()

    expected =
      String.trim_trailing("""
      Slot Start | Slot End | Host                                                             | Port | Role    | Health
      ---------- | -------- | ---------------------------------------------------------------- | ---- | ------- | -------
      0          | 682      | test-redis4-0013-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      0          | 682      | test-redis4-0013-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      683        | 1365     | test-redis4-0001-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      683        | 1365     | test-redis4-0001-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      1366       | 1487     | test-redis4-0002-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      1366       | 1487     | test-redis4-0002-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      1488       | 2170     | test-redis4-0014-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      1488       | 2170     | test-redis4-0014-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      2171       | 2731     | test-redis4-0002-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      2171       | 2731     | test-redis4-0002-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      2732       | 3414     | test-redis4-0015-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      2732       | 3414     | test-redis4-0015-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      3415       | 4097     | test-redis4-0003-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      3415       | 4097     | test-redis4-0003-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      4098       | 4780     | test-redis4-0016-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      4098       | 4780     | test-redis4-0016-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      4781       | 5463     | test-redis4-0004-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      4781       | 5463     | test-redis4-0004-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      5464       | 6146     | test-redis4-0005-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      5464       | 6146     | test-redis4-0005-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      6147       | 6828     | test-redis4-0017-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      6147       | 6828     | test-redis4-0017-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      6829       | 7511     | test-redis4-0006-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      6829       | 7511     | test-redis4-0006-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      7512       | 8193     | test-redis4-0018-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      7512       | 8193     | test-redis4-0018-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      8194       | 8875     | test-redis4-0019-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      8194       | 8875     | test-redis4-0019-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      8876       | 9558     | test-redis4-0007-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      8876       | 9558     | test-redis4-0007-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      9559       | 10240    | test-redis4-0020-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      9559       | 10240    | test-redis4-0020-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      10241      | 10923    | test-redis4-0008-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      10241      | 10923    | test-redis4-0008-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      10924      | 11605    | test-redis4-0021-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      10924      | 11605    | test-redis4-0021-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      11606      | 12288    | test-redis4-0009-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      11606      | 12288    | test-redis4-0009-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      12289      | 12340    | test-redis4-0010-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      12289      | 12340    | test-redis4-0010-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      12341      | 13022    | test-redis4-0022-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      12341      | 13022    | test-redis4-0022-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      13023      | 13653    | test-redis4-0010-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      13023      | 13653    | test-redis4-0010-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      13654      | 14335    | test-redis4-0023-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      13654      | 14335    | test-redis4-0023-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      14336      | 15018    | test-redis4-0011-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      14336      | 15018    | test-redis4-0011-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      15019      | 15620    | test-redis4-0012-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      15019      | 15620    | test-redis4-0012-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      15621      | 16302    | test-redis4-0024-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      15621      | 16302    | test-redis4-0024-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      16303      | 16383    | test-redis4-0012-001.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | master  | unknown
      16303      | 16383    | test-redis4-0012-002.test-redis4.abcdef.usw2.cache.amazonaws.com | 6379 | replica | unknown
      """)

    assert result == expected
  end

  def basic_data() do
    [
      [
        0,
        5460,
        ["127.0.0.1", 6379, "bc169eaf5f94bf1a6a0acfbd9586c492e40c4304", []],
        ["127.0.0.1", 6383, "bfe62c63cc9c2658f0ecb4e73b7ca08a977718e5", []],
        ["127.0.0.1", 6389, "2cb2c73b0893918e8489fc2f0d386ec39e16b5b9", []],
        ["127.0.0.1", 6384, "a1e434afc228a4e98f08529ae4854f19e123862e", []]
      ],
      [
        5461,
        10922,
        ["127.0.0.1", 6380, "303a39cc55db216e49b417d46e8fe4a32e30c783", []],
        ["127.0.0.1", 6386, "b97be4cf4d1dd22404127b11e4f5da71914c28cf", []],
        ["127.0.0.1", 6382, "38c2c740ef09d23beff1d9042e9f379fa3283f66", []],
        ["127.0.0.1", 6385, "93ca1f57cfd0fea36dd77112004e3ec0e6d39dfe", []]
      ],
      [
        10923,
        16383,
        ["127.0.0.1", 6381, "1801cc5f427b6aeea863303cb2d7ece98db4ae1d", []],
        ["127.0.0.1", 6388, "84ec29261ddb384ab0967a6bdfccabcbda67a6ad", []],
        ["127.0.0.1", 6390, "00a722cd4715290b8fd45fe06a73502d9b3fc06d", []],
        ["127.0.0.1", 6387, "66d0470b1a5391208fa9e6c358bc75ebf9d377bf", []]
      ]
    ]
  end

  defp fragmented_data() do
    [
      [
        0,
        682,
        [
          "test-redis4-0013-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "3a01dcc3b30d3177e70a7778983733354fde3f02",
          ["ip", "172.40.62.232"]
        ],
        [
          "test-redis4-0013-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "1059d09031eadda7b83571763bd77ffdd8b7816a",
          ["ip", "172.40.36.0"]
        ]
      ],
      [
        683,
        1365,
        [
          "test-redis4-0001-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "364c32b7731bcadb23d9582b8346ce5b56955b20",
          ["ip", "172.40.33.81"]
        ],
        [
          "test-redis4-0001-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "0a0464f5fd4e2815acbcedad145bf8f3606f6653",
          ["ip", "172.40.26.169"]
        ]
      ],
      [
        1366,
        1487,
        [
          "test-redis4-0002-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "198801cdd9c601c971ecf07de3b6e9b819386137",
          ["ip", "172.40.28.245"]
        ],
        [
          "test-redis4-0002-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "0d7bdbda3b59748756aff05f096c2901a8ad51ab",
          ["ip", "172.40.37.252"]
        ]
      ],
      [
        1488,
        2170,
        [
          "test-redis4-0014-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "2bca81e0f3a70394a50a9090c32c04f4833d29c3",
          ["ip", "172.40.28.242"]
        ],
        [
          "test-redis4-0014-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "ccf7896b74baad0566b51b23417f4058d91fd38d",
          ["ip", "172.40.49.97"]
        ]
      ],
      [
        2171,
        2731,
        [
          "test-redis4-0002-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "198801cdd9c601c971ecf07de3b6e9b819386137",
          ["ip", "172.40.28.245"]
        ],
        [
          "test-redis4-0002-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "0d7bdbda3b59748756aff05f096c2901a8ad51ab",
          ["ip", "172.40.37.252"]
        ]
      ],
      [
        2732,
        3414,
        [
          "test-redis4-0015-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "e42f33131c63f51590cae8038130c441a8693da1",
          ["ip", "172.40.55.182"]
        ],
        [
          "test-redis4-0015-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "51bc65d76982445caa6828bcdd446963669201ee",
          ["ip", "172.40.37.251"]
        ]
      ],
      [
        3415,
        4097,
        [
          "test-redis4-0003-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "beec7914066dd6be71ffed74eeb30bb99eb73850",
          ["ip", "172.40.36.160"]
        ],
        [
          "test-redis4-0003-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "e9881e7ff1fd9d4c16229e3a7f0a08fceda07dd2",
          ["ip", "172.40.20.19"]
        ]
      ],
      [
        4098,
        4780,
        [
          "test-redis4-0016-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "06ead0643819963959dfdb6bad0295ee9bc604fe",
          ["ip", "172.40.61.204"]
        ],
        [
          "test-redis4-0016-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "ffea41a4a4ba7e74299b81045cef7be3b69aa1fc",
          ["ip", "172.40.17.44"]
        ]
      ],
      [
        4781,
        5463,
        [
          "test-redis4-0004-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "f04b3eddfd1cda448b000e540eefd7977f1a3995",
          ["ip", "172.40.33.206"]
        ],
        [
          "test-redis4-0004-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "af8f0cc54b8e1a2ae33f3743bb9154e42d09979f",
          ["ip", "172.40.28.182"]
        ]
      ],
      [
        5464,
        6146,
        [
          "test-redis4-0005-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "85d20bb52b42d1e3a3d207d86a673663d389fbd4",
          ["ip", "172.40.27.209"]
        ],
        [
          "test-redis4-0005-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "a24b0c7345920e0e4edbc18414446724073d3ac9",
          ["ip", "172.40.33.178"]
        ]
      ],
      [
        6147,
        6828,
        [
          "test-redis4-0017-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "762eba16fe8c95f046f028f9b3b92f086d08cc43",
          ["ip", "172.40.39.132"]
        ],
        [
          "test-redis4-0017-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "c809fff4f46fac2633e206c60341f29cfefce49e",
          ["ip", "172.40.61.88"]
        ]
      ],
      [
        6829,
        7511,
        [
          "test-redis4-0006-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "d60a5cf69ff85d1d88b43dc9d6f748638ef7bf2a",
          ["ip", "172.40.20.92"]
        ],
        [
          "test-redis4-0006-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "121570004d9fdf7955b108941072efc016610ac7",
          ["ip", "172.40.43.135"]
        ]
      ],
      [
        7512,
        8193,
        [
          "test-redis4-0018-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "93f73800619216da581e37165bbbb6b7c60af41f",
          ["ip", "172.40.27.69"]
        ],
        [
          "test-redis4-0018-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "26ffbb81af26d3edc189a7d65592bcbb6fd4c303",
          ["ip", "172.40.53.20"]
        ]
      ],
      [
        8194,
        8875,
        [
          "test-redis4-0019-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "6f7de9cf825b2effa0e0af1ee254cccd33031871",
          ["ip", "172.40.43.186"]
        ],
        [
          "test-redis4-0019-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "f0e57bf42d652689f660261765b869a185c48c6a",
          ["ip", "172.40.53.16"]
        ]
      ],
      [
        8876,
        9558,
        [
          "test-redis4-0007-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "0f492b2573fb355e784601712593683fd486823e",
          ["ip", "172.40.23.25"]
        ],
        [
          "test-redis4-0007-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "528a022402240f8e56914827d3be53367117cf39",
          ["ip", "172.40.46.1"]
        ]
      ],
      [
        9559,
        10240,
        [
          "test-redis4-0020-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "ef6ba8900cd63fad010a9f3e44eb7a08a82f8d67",
          ["ip", "172.40.24.58"]
        ],
        [
          "test-redis4-0020-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "476e1628138b834a61808eafc8dd721a71e27500",
          ["ip", "172.40.59.37"]
        ]
      ],
      [
        10241,
        10923,
        [
          "test-redis4-0008-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "44a88d0b4f3b7cb738f7d3558c6b1bc07b209419",
          ["ip", "172.40.19.190"]
        ],
        [
          "test-redis4-0008-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "79ed57dc0fd145f65145b856e793688f1e52825f",
          ["ip", "172.40.42.5"]
        ]
      ],
      [
        10924,
        11605,
        [
          "test-redis4-0021-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "66bbd1d85fd3ebfc3ce85d97beaad35d49341819",
          ["ip", "172.40.35.244"]
        ],
        [
          "test-redis4-0021-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "c5f9f2a6c8dc19c23f39b13568a2ad06fbe3b714",
          ["ip", "172.40.57.45"]
        ]
      ],
      [
        11606,
        12288,
        [
          "test-redis4-0009-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "02ff27163ecb22760add0190cb2c5397bfa0b7cb",
          ["ip", "172.40.46.24"]
        ],
        [
          "test-redis4-0009-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "158ca798f63eb2cc72c3d7488f62dc9de9efcecd",
          ["ip", "172.40.26.238"]
        ]
      ],
      [
        12289,
        12340,
        [
          "test-redis4-0010-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "8db52aeddb6e84253c7a2f25483cffdd8e0e1dda",
          ["ip", "172.40.27.199"]
        ],
        [
          "test-redis4-0010-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "e876fac2eb783410dc8d9041a0e53143caf4e6e4",
          ["ip", "172.40.39.242"]
        ]
      ],
      [
        12341,
        13022,
        [
          "test-redis4-0022-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "3d2f0318fbe740fd6a930cbd0aac50348422d7bf",
          ["ip", "172.40.48.69"]
        ],
        [
          "test-redis4-0022-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "c410edc4a23dda69b7b3a698d46fce3d85573dce",
          ["ip", "172.40.18.240"]
        ]
      ],
      [
        13023,
        13653,
        [
          "test-redis4-0010-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "8db52aeddb6e84253c7a2f25483cffdd8e0e1dda",
          ["ip", "172.40.27.199"]
        ],
        [
          "test-redis4-0010-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "e876fac2eb783410dc8d9041a0e53143caf4e6e4",
          ["ip", "172.40.39.242"]
        ]
      ],
      [
        13654,
        14335,
        [
          "test-redis4-0023-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "38ab9ab762206ef8144b17ff43e604a4618567e0",
          ["ip", "172.40.63.119"]
        ],
        [
          "test-redis4-0023-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "b914d3b8eaae2840b6301dc47432cf9c766adee9",
          ["ip", "172.40.38.179"]
        ]
      ],
      [
        14336,
        15018,
        [
          "test-redis4-0011-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "ed7d2ba82050c48a0d3f62ea09081b3a232e9061",
          ["ip", "172.40.44.147"]
        ],
        [
          "test-redis4-0011-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "22a90411dbf9f939f4e1e81db953e95d6af42bc2",
          ["ip", "172.40.27.36"]
        ]
      ],
      [
        15019,
        15620,
        [
          "test-redis4-0012-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "a9b722d9b78b135d2dfadb5f46d7572413de577a",
          ["ip", "172.40.39.217"]
        ],
        [
          "test-redis4-0012-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "b2b902de28e7bc2aba4444477b3c7e3ffaf27120",
          ["ip", "172.40.31.18"]
        ]
      ],
      [
        15621,
        16302,
        [
          "test-redis4-0024-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "b233edf38a1afed6a8a595d4c4ce1d694a93e428",
          ["ip", "172.40.31.88"]
        ],
        [
          "test-redis4-0024-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "83c873aeee139f4e30c9c4a2d33dea0cfefe54b1",
          ["ip", "172.40.54.19"]
        ]
      ],
      [
        16303,
        16383,
        [
          "test-redis4-0012-001.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "a9b722d9b78b135d2dfadb5f46d7572413de577a",
          ["ip", "172.40.39.217"]
        ],
        [
          "test-redis4-0012-002.test-redis4.abcdef.usw2.cache.amazonaws.com",
          6379,
          "b2b902de28e7bc2aba4444477b3c7e3ffaf27120",
          ["ip", "172.40.31.18"]
        ]
      ]
    ]
  end

  defp normalize_table(table_string) do
    table_string
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.join("\n")
  end
end
