import gradio as gr
import pandas as pd
from datetime import timedelta
from src.verify.leaderboard import leaderboard
from src.utils.db_utils import fetch_df

def load_errors(days=7):
    sql = """
    SELECT source, variable, horizon_hours, valid_time, mae, rmse, mape
    FROM errors
    WHERE valid_time >= now() - interval '7 days'
    """
    return fetch_df(sql)

def tab_verification():
    df = load_errors()
    if df.empty:
        return gr.HTML("<p>No data yet. Please check back later.</p>")
    piv = df.groupby(["variable","horizon_hours","source"]).agg({"rmse":"mean","mae":"mean"}).reset_index()
    return piv

def tab_leaderboard():
    lb = leaderboard(7)
    return lb

def tab_our_vs_best():
    df = load_errors()
    if df.empty: return df
    best = leaderboard(7).rename(columns={"source":"best_source"})
    merged = df.merge(best[["variable","horizon_hours","best_source"]], on=["variable","horizon_hours"], how="left")
    our = merged[merged["source"]=="our_model"].rename(columns={"rmse":"rmse_our","mae":"mae_our"})
    bestm = merged.merge(our[["variable","horizon_hours","rmse_our","mae_our"]], on=["variable","horizon_hours"], how="left")
    bestm = bestm[bestm["source"]==bestm["best_source"]]
    bestm["rmse_diff"] = bestm["rmse_our"] - bestm["rmse"]
    bestm["mae_diff"] = bestm["mae_our"] - bestm["mae"]
    return bestm[["variable","horizon_hours","best_source","rmse","rmse_our","rmse_diff","mae","mae_our","mae_diff"]]

def tab_drift():
    df = load_errors()
    if df.empty: return df
    recent = df.groupby(["variable","source"]).resample("12H", on="valid_time").rmse.mean().reset_index()
    return recent

def app():
    with gr.Blocks(title="Weather Forecast Verification") as demo:
        gr.Markdown("# Weather Forecast Verification Dashboard")
        with gr.Tab("Verification"):
            out1 = gr.Dataframe(interactive=False)
            out1.value = tab_verification()
        with gr.Tab("Leaderboard"):
            out2 = gr.Dataframe(interactive=False)
            out2.value = tab_leaderboard()
        with gr.Tab("Our vs Best"):
            out3 = gr.Dataframe(interactive=False)
            out3.value = tab_our_vs_best()
        with gr.Tab("Drift"):
            out4 = gr.Dataframe(interactive=False)
            out4.value = tab_drift()
    return demo

if __name__ == "__main__":
    app().launch(server_name="0.0.0.0", server_port=7860)

