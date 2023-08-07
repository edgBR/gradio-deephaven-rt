import gradio as gr
#from datarepository import get_data
from datarepository import BTCfetcher



with gr.Blocks() as demo:
    gr.Markdown("# 💉 BTC 1second update")
    with gr.Row():
        gr.ScatterPlot(BTCfetcher().run(), every=60, x="time_10s", 
                        y="avg_price", width=500, height=500)
        

demo.queue().launch(share=True)  # Run the demo with queuing enabled