import pandas as pd
import pytest
from io import StringIO


def validate_dataframe(df):
    mask = (
        df["product_id"].notna()
        & (df["product_id"].astype(str).str.strip() != "")
        & (df["price"] > 0)
        & (df["supplier"].notna())
        & (df["supplier"].astype(str).str.strip() != "")
        & (df["supplier"] != "Unknown")
    )
    return df[mask].copy(), df[~mask].copy()


def test_valid_rows_pass():
    csv_data = """product_id,name,description,price,supplier,stock,created_at
RSP-001,Valid Pen,Desc,100,Resparked,10,2024-01-01
RSP-004,Valid Kit,Desc,500,Resparked,5,2024-01-01
"""
    df = pd.read_csv(StringIO(csv_data))
    valid, invalid = validate_dataframe(df)
    assert len(valid) == 2
    assert len(invalid) == 0


def test_invalid_rows_filtered():
    csv_data = """product_id,name,description,price,supplier,stock,created_at
RSP-001,Valid Pen,Desc,100,Resparked,10,2024-01-01
,No ID Pen,Desc,100,Resparked,10,2024-01-01
RSP-002,Zero Price,Desc,0,Resparked,10,2024-01-01
RSP-003,Negative Price,Desc,-100,Resparked,10,2024-01-01
RSP-004,Unknown Supplier,Desc,100,Unknown,10,2024-01-01
RSP-005,Empty Supplier,Desc,100,,10,2024-01-01
"""
    df = pd.read_csv(StringIO(csv_data))
    valid, invalid = validate_dataframe(df)

    assert len(valid) == 1
    assert "RSP-001" in valid["product_id"].values
    assert len(invalid) == 5


def test_double_error_row():
    """Fila con product_id vacío Y price negativo — caso fila-46 del CSV real."""
    csv_data = """product_id,name,description,price,supplier,stock,created_at
,Double Error,Desc,-50,Resparked,10,2024-01-01
RSP-001,Valid,Desc,100,Resparked,10,2024-01-01
"""
    df = pd.read_csv(StringIO(csv_data))
    valid, invalid = validate_dataframe(df)
    assert len(valid) == 1
    assert len(invalid) == 1


def test_empty_dataframe():
    df = pd.DataFrame(
        columns=[
            "product_id",
            "name",
            "description",
            "price",
            "supplier",
            "stock",
            "created_at",
        ]
    )
    valid, invalid = validate_dataframe(df)
    assert len(valid) == 0
    assert len(invalid) == 0


def test_all_rows_valid():
    """Todos válidos — ninguno debe ir a invalid."""
    csv_data = """product_id,name,description,price,supplier,stock,created_at
RSP-001,Pen A,Desc,100,Resparked,10,2024-01-01
RSP-002,Pen B,Desc,200,GrabaTech,5,2024-01-01
"""
    df = pd.read_csv(StringIO(csv_data))
    valid, invalid = validate_dataframe(df)
    assert len(valid) == 2
    assert len(invalid) == 0
