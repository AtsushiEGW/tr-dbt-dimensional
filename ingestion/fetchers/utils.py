from __future__ import annotations

import csv
import tempfile
from pathlib import Path
from typing import Optional


def clean_csv_trailing_commas(
    src_path: Path | str,
    dst_path: Optional[Path | str] = None,
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    meta_rows: int = 0,
    src_encoding: str = "utf-8",
    dst_encoding: str = "utf-8",
    newline: str = ""  # csv では空文字指定が推奨
) -> Path:
    """
    末尾の余計なカンマ（= 末尾の空カラム）を安全に削除して新しいCSVを書き出す。

    ポリシー:
      - ヘッダ行（meta_rows 行スキップ後の1行）を基準に「正しい列数」を決める
      - データ行で列数がヘッダより多い場合、**末尾側の空文字列のみ**を切り詰めて列数を合わせる
        * 途中にある空文字列は保持（値の欠損はデータとして重要なため）
      - メタ行はそのまま書き戻す（加工しない）
      - ヘッダ列数より「本当に列が多い」データ（末尾が空ではない）は**何も変更しない**
        （上流の抽出仕様が変わっている可能性があるため、勝手に削らない）

    Args:
        src_path: 入力CSV
        dst_path: 出力CSV（省略時は src と同じフォルダに *.clean.csv を作成）
        delimiter, quotechar: CSV の区切り/引用
        meta_rows: ヘッダの手前にあるメタ行の数（例: 1 なら先頭1行はそのまま残す）
        src_encoding: 入力のエンコーディング
        dst_encoding: 出力のエンコーディング
        newline: csv.open の推奨値は ""（そのまま）

    Returns:
        出力先 Path
    """
    src = Path(src_path)
    if dst_path is None:
        dst = src.with_suffix(src.suffix + ".clean.csv")
    else:
        dst = Path(dst_path)

    # まずはヘッダ列数を確定
    with src.open("r", encoding=src_encoding, newline=newline) as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        # メタ行スキップ
        meta_buf: list[list[str]] = []
        for _ in range(meta_rows):
            try:
                meta_buf.append(next(reader))
            except StopIteration:
                # ファイルが短すぎる場合は空で出力して返す
                dst.write_text("", encoding=dst_encoding)
                return dst

        try:
            header = next(reader)
        except StopIteration:
            # メタ行しか無い
            with dst.open("w", encoding=dst_encoding, newline=newline) as out:
                w = csv.writer(out, delimiter=delimiter, quotechar=quotechar)
                for row in meta_buf:
                    w.writerow(row)
            return dst

        header_len = len(header)
        # 以降を全行バッファ（容量が大きい場合はストリーミングが望ましいが、ここは実装簡便性を優先）
        data_rows = [row for row in reader]

    # 出力
    with dst.open("w", encoding=dst_encoding, newline=newline) as out:
        writer = csv.writer(out, delimiter=delimiter, quotechar=quotechar)
        # メタ行はそのまま
        for row in meta_buf:
            writer.writerow(row)
        # ヘッダ
        writer.writerow(header)

        # データ行
        for row in data_rows:
            if len(row) > header_len:
                # 末尾側の空要素を落として header_len 以下に詰める
                # ただし「末尾が空でない」= 実データが追加された行は触らない
                trimmed = row[:]
                while len(trimmed) > header_len and (trimmed[-1] == "" or trimmed[-1] is None):
                    trimmed.pop()

                # それでも多ければ触らない（仕様変更などの可能性）
                if len(trimmed) == header_len:
                    writer.writerow(trimmed)
                else:
                    writer.writerow(row)
            else:
                writer.writerow(row)

    return dst


def clean_csv_trailing_commas_inplace(
    path: Path | str,
    **kwargs
) -> Path:
    """
    上記 clean_csv_trailing_commas を in-place で実行するヘルパー。
    一時ファイルを作ってから置き換え、元ファイルは .bak に退避。
    """
    src = Path(path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_out = Path(tmpdir) / (src.name + ".clean.csv")
        out = clean_csv_trailing_commas(src, tmp_out, **kwargs)
        bak = src.with_suffix(src.suffix + ".bak")
        if bak.exists():
            bak.unlink()
        src.rename(bak)
        out.replace(src)
    return src