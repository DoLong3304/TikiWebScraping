"""Tkinter GUI wrapper for the Tiki scraping pipeline.

The GUI keeps the existing CLI untouched while exposing quick actions such
as connection checks, settings, run controls, stats, and a lightweight SQL
playground for Supabase.
"""
from __future__ import annotations

import threading
import queue
import logging
import tkinter as tk
from tkinter import ttk, messagebox
from typing import List

from src.gui.pipeline_runner import PipelineRunner, RunPlan, RuntimeSettings


class GuiLogHandler(logging.Handler):
    """Push log records into a queue so the UI can render them."""

    def __init__(self, log_queue: queue.Queue[str]):
        super().__init__()
        self.log_queue = log_queue
        self.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - UI glue
        try:
            msg = self.format(record)
            self.log_queue.put(msg)
        except Exception:
            pass


class GuiApp:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("Tiki Pipeline Control Panel")
        self.root.geometry("1100x750")

        self.runner = PipelineRunner()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self._wire_logging()

        self._build_layout()
        self._poll_log_queue()

    # ------------------------------------------------------------------
    # UI assembly
    # ------------------------------------------------------------------
    def _wire_logging(self) -> None:
        handler = GuiLogHandler(self.log_queue)
        handler.setLevel(logging.INFO)
        for name in ("", "tiki_pipeline", "tiki_gui"):
            logger = logging.getLogger(name)
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)

    def _build_layout(self) -> None:
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.connection_tab = ttk.Frame(notebook)
        self.settings_tab = ttk.Frame(notebook)
        self.run_tab = ttk.Frame(notebook)
        self.stats_tab = ttk.Frame(notebook)
        self.sql_tab = ttk.Frame(notebook)

        notebook.add(self.connection_tab, text="Connections")
        notebook.add(self.settings_tab, text="Settings")
        notebook.add(self.run_tab, text="Run")
        notebook.add(self.stats_tab, text="Stats")
        notebook.add(self.sql_tab, text="SQL editor")

        self._build_connections_tab()
        self._build_settings_tab()
        self._build_run_tab()
        self._build_stats_tab()
        self._build_sql_tab()

    # ------------------------------------------------------------------
    # Connections tab
    # ------------------------------------------------------------------
    def _build_connections_tab(self) -> None:
        frame = self.connection_tab
        ttk.Label(frame, text="Connection checks", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, sticky="w", pady=(6, 4))
        ttk.Label(
            frame,
            text="Run these quick checks before starting. Both should say reachable.",
            foreground="#333",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 8))

        self.tiki_status = tk.StringVar(value="Not tested")
        self.supabase_status = tk.StringVar(value="Not tested")
        ttk.Button(frame, text="Test Tiki API", command=self._on_test_tiki).grid(row=2, column=0, sticky="w", pady=4)
        ttk.Label(frame, textvariable=self.tiki_status).grid(row=2, column=1, sticky="w", padx=8)

        ttk.Button(frame, text="Test Supabase API", command=self._on_test_supabase).grid(row=3, column=0, sticky="w", pady=4)
        ttk.Label(frame, textvariable=self.supabase_status).grid(row=3, column=1, sticky="w", padx=8)

    def _on_test_tiki(self) -> None:
        ok, msg = self.runner.test_tiki_connection()
        self.tiki_status.set(msg)
        if not ok:
            messagebox.showerror("Tiki API", msg)

    def _on_test_supabase(self) -> None:
        ok, msg = self.runner.test_supabase_connection()
        self.supabase_status.set(msg)
        if not ok:
            messagebox.showerror("Supabase API", msg)

    # ------------------------------------------------------------------
    # Settings tab
    # ------------------------------------------------------------------
    def _build_settings_tab(self) -> None:
        frame = self.settings_tab
        frame.columnconfigure(1, weight=1)
        ttk.Label(frame, text="Runtime settings", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(6, 8))
        ttk.Label(
            frame,
            text="Tweak limits and timing here. Safe defaults are already filled in.",
            foreground="#333",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 8))

        self.var_parent = tk.IntVar(value=self.runner.settings.parent_category_id)
        self.var_max_pages = tk.IntVar(value=self.runner.settings.max_pages_per_category)
        self.var_max_review_pages = tk.IntVar(value=self.runner.settings.max_review_pages_per_product)
        self.var_base_delay = tk.DoubleVar(value=self.runner.settings.base_delay_seconds)
        self.var_jitter = tk.DoubleVar(value=self.runner.settings.jitter_range)
        self.var_start_index = tk.IntVar(value=self.runner.settings.start_index_reviews)
        self.var_stats_limit = tk.IntVar(value=self.runner.settings.stats_category_limit)

        row = 2
        for label, var in [
            ("Parent category id", self.var_parent),
            ("Max pages / category", self.var_max_pages),
            ("Max review pages", self.var_max_review_pages),
            ("Base delay (s)", self.var_base_delay),
            ("Jitter range", self.var_jitter),
            ("Review start index", self.var_start_index),
            ("Stats leaf cap", self.var_stats_limit),
        ]:
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=3)
            ttk.Entry(frame, textvariable=var, width=20).grid(row=row, column=1, sticky="w", pady=3)
            row += 1

        ttk.Button(frame, text="Apply settings", command=self._on_apply_settings).grid(row=row, column=0, sticky="w", pady=8)

    def _on_apply_settings(self) -> None:
        self.runner.settings = RuntimeSettings(
            parent_category_id=self.var_parent.get(),
            max_pages_per_category=self.var_max_pages.get(),
            max_review_pages_per_product=self.var_max_review_pages.get(),
            base_delay_seconds=self.var_base_delay.get(),
            jitter_range=self.var_jitter.get(),
            start_index_reviews=self.var_start_index.get(),
            stats_category_limit=max(1, self.var_stats_limit.get()),
        )
        self.runner.settings.apply_to_config()
        messagebox.showinfo("Settings", "Settings applied for future runs.")
        self._update_run_summary()

    # ------------------------------------------------------------------
    # Run tab
    # ------------------------------------------------------------------
    def _build_run_tab(self) -> None:
        frame = self.run_tab
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(3, weight=1)

        controls = ttk.LabelFrame(frame, text="Run plan")
        controls.grid(row=0, column=0, sticky="ew", padx=4, pady=4)
        self.stage_options = [
            ("1. Categories + Listings", "categories_listings"),
            ("2. Products", "products"),
            ("3. Reviews", "reviews"),
            ("4. Sellers", "sellers"),
        ]
        self.stage_listbox = tk.Listbox(controls, selectmode=tk.MULTIPLE, exportselection=False, height=4)
        for idx, (label, _) in enumerate(self.stage_options):
            self.stage_listbox.insert(idx, label)
            self.stage_listbox.selection_set(idx)
        ttk.Label(controls, text="Select stages (top to bottom order)").grid(row=0, column=0, sticky="w", padx=4, pady=2)
        self.stage_listbox.grid(row=1, column=0, rowspan=4, sticky="w", padx=4, pady=2)

        self.var_mode = tk.StringVar(value="scrape")
        ttk.Label(controls, text="Mode").grid(row=0, column=1, sticky="w", padx=4, pady=2)
        ttk.Radiobutton(controls, text="Scrape new + update", variable=self.var_mode, value="scrape").grid(row=1, column=1, sticky="w", padx=4, pady=2)
        ttk.Radiobutton(controls, text="Update existing only", variable=self.var_mode, value="update").grid(row=2, column=1, sticky="w", padx=4, pady=2)

        self.var_product_override = tk.StringVar()
        ttk.Label(controls, text="Product IDs (comma) to focus").grid(row=3, column=1, sticky="w", padx=4, pady=2)
        ttk.Entry(controls, textvariable=self.var_product_override, width=50).grid(row=4, column=1, sticky="w", padx=4, pady=2)

        ttk.Button(controls, text="Run selected", command=self._on_run_selected).grid(row=5, column=0, padx=4, pady=6, sticky="w")
        ttk.Button(controls, text="Retry failed reviews", command=self._on_retry_failed).grid(row=5, column=1, padx=4, pady=6, sticky="w")
        ttk.Button(controls, text="Clear logs", command=self._clear_logs).grid(row=5, column=2, padx=4, pady=6, sticky="w")

        self.stage_listbox.bind("<<ListboxSelect>>", lambda _: self._update_run_summary())
        self.var_mode.trace_add("write", lambda *_: self._update_run_summary())
        self.var_product_override.trace_add("write", lambda *_: self._update_run_summary())

        # Helper card to make the flow clearer for non-tech users
        tips = ttk.LabelFrame(frame, text="What will happen")
        tips.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 6))
        tips.columnconfigure(0, weight=1)
        self.var_run_summary = tk.StringVar(value="All stages selected. Scrape new data and update existing records.")
        ttk.Label(
            tips,
            text="Stages always run in this order: Categories/Listings → Products → Reviews → Sellers.",
            foreground="#333",
        ).grid(row=0, column=0, sticky="w", padx=6, pady=(4, 2))
        ttk.Label(tips, textvariable=self.var_run_summary, foreground="#005a9c", wraplength=950, justify=tk.LEFT).grid(row=1, column=0, sticky="w", padx=6, pady=(0, 4))
        ttk.Label(
            tips,
            text="Tip: Leave product IDs empty to cover everything. Use Update mode when you only want to refresh what's already saved.",
            foreground="#333",
            wraplength=950,
            justify=tk.LEFT,
        ).grid(row=2, column=0, sticky="w", padx=6, pady=(0, 6))

        self._update_run_summary()

        # Log panel
        log_frame = ttk.LabelFrame(frame, text="Interactive logs")
        log_frame.grid(row=3, column=0, sticky="nsew", padx=4, pady=4)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, wrap="word", state="disabled", height=25)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text["yscrollcommand"] = scrollbar.set

    def _plan_from_form(self) -> RunPlan:
        product_ids: List[int] = []
        override = self.var_product_override.get().strip()
        if override:
            try:
                product_ids = [int(x.strip()) for x in override.split(",") if x.strip()]
            except ValueError:
                messagebox.showerror("Run plan", "Product IDs must be integers separated by commas")
                return None  # type: ignore

        selected_indices = set(self.stage_listbox.curselection())
        selected_keys = {self.stage_options[i][1] for i in selected_indices}
        if not selected_keys:
            messagebox.showerror("Run plan", "Select at least one stage to run")
            return None  # type: ignore

        mode = self.var_mode.get()
        needs_source = mode == "scrape" and ("products" in selected_keys or "reviews" in selected_keys or "sellers" in selected_keys)
        if needs_source and "categories_listings" not in selected_keys and not product_ids:
            messagebox.showerror(
                "Run plan",
                "Scrape mode needs 'Categories + Listings' selected or explicit product IDs",
            )
            return None  # type: ignore

        return RunPlan(
            categories_listings="categories_listings" in selected_keys,
            products="products" in selected_keys,
            reviews="reviews" in selected_keys,
            sellers="sellers" in selected_keys,
            mode=mode,
            product_ids_override=product_ids or None,
            start_index_reviews=self.runner.settings.start_index_reviews,
        )

    def _update_run_summary(self) -> None:
        selected_indices = set(self.stage_listbox.curselection())
        selected_labels = [self.stage_options[i][0] for i in selected_indices]
        if not selected_labels:
            self.var_run_summary.set("Select at least one stage to build a run plan.")
            return

        mode = self.var_mode.get()
        override = self.var_product_override.get().strip()
        mode_text = "Scrape new data + update existing" if mode == "scrape" else "Update existing only (no new products)"
        scope_text = "all products and categories" if not override else f"products: {override}"
        start_idx = self.runner.settings.start_index_reviews
        review_hint = f"reviews start at product index {start_idx}" if start_idx else "reviews start from the first product"
        parent_hint = f"parent category id {self.runner.settings.parent_category_id}"

        summary = (
            f"Mode: {mode_text}. Stages: {', '.join(selected_labels)}. "
            f"Scope: {scope_text}; {review_hint}; {parent_hint}."
        )
        self.var_run_summary.set(summary)

    def _on_run_selected(self) -> None:
        plan = self._plan_from_form()
        if plan is None:
            return
        self._run_in_thread(lambda: self._execute_plan(plan))

    def _execute_plan(self, plan: RunPlan) -> None:
        logging.info("Starting run: %s", plan)
        errors = self.runner.run_plan(plan)
        issues = sum(len(v) for v in errors.values())
        if issues:
            logging.warning("Run completed with %d issue(s). See log.", issues)
        else:
            logging.info("Run completed successfully.")

    def _on_retry_failed(self) -> None:
        if not self.runner.failed_review_ids:
            messagebox.showinfo("Retry", "No failed review product IDs recorded yet.")
            return
        self._run_in_thread(self.runner.retry_failed_reviews)

    # ------------------------------------------------------------------
    # Stats tab
    # ------------------------------------------------------------------
    def _build_stats_tab(self) -> None:
        frame = self.stats_tab
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text="Tiki vs Supabase", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, sticky="w", pady=(6, 4))

        self.var_stats_text = tk.StringVar(value="Click refresh to fetch stats")
        ttk.Label(frame, textvariable=self.var_stats_text, justify=tk.LEFT).grid(row=1, column=0, sticky="w", padx=4, pady=4)

        ttk.Button(frame, text="Refresh stats", command=self._on_refresh_stats).grid(row=2, column=0, sticky="w", padx=4, pady=4)

    def _on_refresh_stats(self) -> None:
        def _work() -> None:
            stats = self.runner.refresh_stats()
            text = (
                f"Tiki (est): categories={stats['tiki'].get('categories', 0)}, "
                f"products≈{stats['tiki'].get('products_estimate', 0)}\n"
                f"Supabase: categories={stats['supabase'].get('categories', 0)}, "
                f"products={stats['supabase'].get('products', 0)}, "
                f"sellers={stats['supabase'].get('sellers', 0)}, "
                f"reviews={stats['supabase'].get('reviews', 0)}"
            )
            self.var_stats_text.set(text)

        self._run_in_thread(_work)

    # ------------------------------------------------------------------
    # SQL tab
    # ------------------------------------------------------------------
    def _build_sql_tab(self) -> None:
        frame = self.sql_tab
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        ttk.Label(frame, text="Supabase SQL (safe subset)", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, sticky="w", pady=(6, 4))

        self.sql_text = tk.Text(frame, height=6)
        self.sql_text.insert("1.0", "SELECT * FROM product LIMIT 5")
        self.sql_text.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)

        ttk.Button(frame, text="Run query", command=self._on_run_sql).grid(row=2, column=0, sticky="w", padx=4, pady=4)

        self.sql_result = tk.Text(frame, height=18, state="disabled")
        self.sql_result.grid(row=3, column=0, sticky="nsew", padx=4, pady=4)

    def _on_run_sql(self) -> None:
        query = self.sql_text.get("1.0", tk.END).strip()
        ok, msg, rows = self.runner.run_sql(query)
        self.sql_result.configure(state="normal")
        self.sql_result.delete("1.0", tk.END)
        self.sql_result.insert(tk.END, msg + "\n")
        if rows:
            for row in rows:
                self.sql_result.insert(tk.END, f"{row}\n")
        self.sql_result.configure(state="disabled")
        if not ok:
            messagebox.showerror("SQL", msg)

    # ------------------------------------------------------------------
    # Log plumbing
    # ------------------------------------------------------------------
    def _poll_log_queue(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self._append_log(msg)
        except queue.Empty:
            pass
        self.root.after(200, self._poll_log_queue)

    def _append_log(self, msg: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _clear_logs(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    # ------------------------------------------------------------------
    # Thread helpers
    # ------------------------------------------------------------------
    def _run_in_thread(self, func) -> None:
        thread = threading.Thread(target=func, daemon=True)
        thread.start()

    # ------------------------------------------------------------------
    def run(self) -> None:
        self.root.mainloop()


def run_gui() -> None:
    app = GuiApp()
    app.run()


if __name__ == "__main__":  # pragma: no cover - manual launch only
    run_gui()
