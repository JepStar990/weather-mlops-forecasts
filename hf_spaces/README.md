# Deploy Gradio Dashboard on Hugging Face Spaces (Free)

1. Create a new Space (Gradio, Python) at https://huggingface.co/spaces
2. Add files:
   - `src/serve/dashboard/app.py`
   - `requirements.txt`
3. In Space **Secrets** add:
   - `DATABASE_URL` (read-only Neon conn string if possible)
4. In `app.py` import path must resolve in Space; either:
   - Copy minimal utils into Space, or
   - Add the repo as a submodule or package. Easiest: copy `src/verify/leaderboard.py`, `src/utils/db_utils.py`, `src/utils/logging_utils.py`.
5. Set Space to public. First render will occur once data flows.
