import marimo

__generated_with = "0.19.9"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    import plotly.express as px
    import os

    def sql(sql_sentence: str) -> pd.DataFrame:
        df = duckdb.query(sql_sentence).to_df()
        return df

    return mo, px, sql


@app.cell
def _(mo, px, sql):
    df = sql("""--sql
    with
    src as (
        select *
        from read_csv_auto('gapminder_data_graphs.csv')
        )
    select
        country, year, gdp,
        avg(gdp) over(
            partition by country 
            order by year 
            rows between 2 preceding and current row
        ) as moving_avg_gdp
    from src

    where country in ('Japan', 'United States', 'China')

    """)


    mo.ui.plotly(
        px.line(
            df,
            x="year",
            y=["gdp", "moving_avg_gdp"],
            color="country",
            line_dash="variable",
            line_dash_map={
                "gdp": "dash",
                "moving_avg_gdp": "solid",
            },
            title="GDP over Time for Selected Countries"
        )
    )

    return


@app.cell
def _(sql):
    sql("""--sql

    with
    src as (
        select *
        from read_csv_auto('gapminder_data_graphs.csv')
        )
    select
        *
    from src

    where country = 'Japan'
    """)

    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        with 
        src as (
            select *
            from read_csv_auto('gapminder_data_graphs.csv')
            )
        select
            *
        from src
        """
    )
    return


if __name__ == "__main__":
    app.run()
