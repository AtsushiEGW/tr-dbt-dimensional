import os
import pandas as pd
from typing import List, Dict

def split_csv_by_ranges(
    input_path: str,
    output_dir: str,
    base_name: str,
    ranges: List[Dict[str, int]],
    encoding: str = "utf-8"
) -> None:
    """
    CSVファイルを指定した行番号の範囲ごとに分割して出力する。

    Parameters
    ----------
    input_path : str
        入力CSVファイルのパス
    output_dir : str
        出力先ディレクトリ
    base_name : str
        出力CSVファイルのベース名（例: "links" -> links_01.csv など）
    ranges : list of dict
        [{"start": 1, "end": 2005}, ...] の形式で指定
        start/end はヘッダーを含めない（データ1行目を start=1 とする）
    encoding : str
        CSVの文字コード
    """
    # 出力フォルダ作成
    os.makedirs(output_dir, exist_ok=True)

    # CSV読み込み
    df = pd.read_csv(input_path, encoding=encoding)
    total_rows = len(df)

    # ファイル番号のゼロ埋め桁数
    pad = max(2, len(str(len(ranges))))

    for i, r in enumerate(ranges, start=1):
        start, end = r["start"], r["end"]

        # 範囲チェック & クランプ
        if start < 1:
            start = 1
        if end < start:
            continue
        end = min(end, total_rows)

        # 抽出
        df_slice = df.iloc[start - 1 : end]
        if df_slice.empty:
            continue

        # 出力ファイル名
        output_path = os.path.join(output_dir, f"{base_name}_{i:0{pad}d}.csv")

        # 書き込み
        df_slice.to_csv(output_path, index=False, encoding=encoding)
        print(f"Created: {output_path} (rows {start}-{end}/{total_rows})")




split_csv_by_ranges(
    input_path="data/csvs_demo/links.csv",
    output_dir="data/csvs_demo/links/",
    base_name="links",
    ranges = [
        {"start": 1, "end": 2005},
        {"start": 2000, "end": 2903},
        {"start": 3000, "end": 4029},
        {"start": 4000, "end": 5029},
        {"start": 4900, "end": 6239},
        {"start": 6000, "end": 8310},
        {"start": 8000, "end": 9743}
    ]
)

split_csv_by_ranges(
    input_path="data/csvs_demo/movies.csv",
    output_dir="data/csvs_demo/movies/",
    base_name="movies",
    ranges = [
        {"start": 1, "end": 2005},
        {"start": 2000, "end": 2903},
        {"start": 3000, "end": 4029},
        {"start": 4000, "end": 5029},
        {"start": 4900, "end": 6239},
        {"start": 6000, "end": 8310},
        {"start": 8000, "end": 9743}
    ]
)

split_csv_by_ranges(
    input_path="data/csvs_demo/ratings.csv",
    output_dir="data/csvs_demo/ratings/",
    base_name="ratings",
    ranges = [
        {"start": 1, "end": 2005},
        {"start": 2000, "end": 2903},
        {"start": 3000, "end": 4029},
        {"start": 4000, "end": 5029},
        {"start": 4900, "end": 6239},
        {"start": 6000, "end": 8310},
        {"start": 8000, "end": 100837}
    ]
)

split_csv_by_ranges(
    input_path="data/csvs_demo/tags.csv",
    output_dir="data/csvs_demo/tags/",
    base_name="tags",
    ranges = [
        {"start": 1, "end": 2005},
        {"start": 2000, "end": 2903},
        {"start": 3000, "end": 3684},
    ]
)