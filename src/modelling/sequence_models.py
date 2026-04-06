"""
Sequence models (LSTM, Transformer) for BigDataSmallPrice – Phase 3.

Both models consume sliding-window sequences of shape (batch, lookback, n_features)
and produce a single scalar prediction per sequence.

Architecture:
- LSTMModel:        2-layer LSTM → Linear(1)
- TransformerModel: Linear projection → TransformerEncoder (Pre-LN) → last-step head

Training:
- Features and target are z-scored with statistics computed on the training set.
- Adam optimiser + ReduceLROnPlateau scheduler + gradient clipping + early stopping.
- Large datasets (Model A: ~462k rows) are subsampled to MAX_TRAIN_SEQS most-recent
  rows before building sequences, keeping wall-clock training time on CPU manageable.

Serialisation:
- Models are saved as .pt bundles via save_torch_model().
- The bundle contains state_dict, architecture config, scaler parameters
  (scaler_mean/scale for X, y_mean/y_scale for the target), feature_cols, and lookback.
- Use load_torch_bundle() + reconstruct_model() to restore them for inference.
- predict_from_sequences() handles the scaled inference pass end-to-end.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset


# ── Dataset ───────────────────────────────────────────────────────────────────


class SequenceDataset(Dataset):
    """
    Lazy sliding-window dataset.

    Stores X and y as tensors and builds (lookback, n_features) windows
    on-the-fly in __getitem__ to avoid the O(N × lookback × features) memory
    cost of pre-building all sequences at once.

    Args:
        X:        Feature array of shape (T, n_features), dtype float32.
        y:        Target array of shape (T,), dtype float32.
        lookback: Number of past steps in each input window.
    """

    def __init__(self, X: np.ndarray, y: np.ndarray, lookback: int) -> None:
        self.X = torch.from_numpy(X.astype(np.float32))
        self.y = torch.from_numpy(y.astype(np.float32))
        self.lookback = lookback

    def __len__(self) -> int:
        return max(0, len(self.X) - self.lookback)

    def __getitem__(self, idx: int):
        return self.X[idx : idx + self.lookback], self.y[idx + self.lookback]


# ── Models ────────────────────────────────────────────────────────────────────


class LSTMModel(nn.Module):
    """
    Two-layer LSTM regressor.

    Args:
        input_size:  Number of input features per time step.
        hidden_size: LSTM hidden-state dimension (default 64).
        num_layers:  Number of stacked LSTM layers (default 2).
        dropout:     Dropout between layers; ignored when num_layers == 1.
    """

    def __init__(
        self,
        input_size: int,
        hidden_size: int = 64,
        num_layers: int = 2,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)              # (B, T, H)
        return self.head(out[:, -1, :]).squeeze(-1)  # (B,)


class TransformerModel(nn.Module):
    """
    Encoder-only Transformer regressor (Pre-LayerNorm variant).

    Args:
        input_size:  Number of input features per time step.
        d_model:     Internal model dimension (default 64).
        nhead:       Number of attention heads; must divide d_model (default 4).
        num_layers:  Number of TransformerEncoderLayer blocks (default 2).
        dropout:     Attention + FFN dropout (default 0.1).
    """

    def __init__(
        self,
        input_size: int,
        d_model: int = 64,
        nhead: int = 4,
        num_layers: int = 2,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_size, d_model)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=256,
            dropout=dropout,
            batch_first=True,
            norm_first=True,    # Pre-LN: more stable training
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.head = nn.Linear(d_model, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.input_proj(x)          # (B, T, d_model)
        x = self.encoder(x)             # (B, T, d_model)
        return self.head(x[:, -1, :]).squeeze(-1)  # (B,) – last time step


# ── Training loop ─────────────────────────────────────────────────────────────


def train_sequence_model(
    model: nn.Module,
    X_train: np.ndarray,
    y_train: np.ndarray,
    lookback: int,
    X_val: np.ndarray | None = None,
    y_val: np.ndarray | None = None,
    epochs: int = 30,
    batch_size: int = 512,
    lr: float = 1e-3,
    patience: int = 7,
    max_train_seqs: int | None = 30_000,
) -> tuple[nn.Module, list[float]]:
    """
    Generic training loop for LSTM / Transformer sequence models.

    When *max_train_seqs* is set and the training data would produce more
    sequences than that limit, only the *most recent* rows are used.  This
    keeps wall-clock training time on CPU manageable for large datasets
    (e.g. Model A with ~462 k rows at 15-minute resolution).

    Args:
        model:          Untrained LSTMModel or TransformerModel.
        X_train:        Scaled feature array (T_train, n_features), float32.
        y_train:        Scaled target array (T_train,), float32.
        lookback:       Sliding-window length.
        X_val:          Optional scaled validation feature context array
                        (lookback + T_val, n_features).
        y_val:          Optional scaled validation target context array
                        (lookback + T_val,).
        epochs:         Maximum training epochs.
        batch_size:     Mini-batch size.
        lr:             Initial Adam learning rate.
        patience:       Early-stopping patience (epochs without val improvement).
        max_train_seqs: Cap on training sequences. None = no cap.

    Returns:
        (model, val_losses) where val_losses is a list of per-epoch val MSE
        values (empty list if no validation set was provided).
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"  Training device: {device}" + (f" ({torch.cuda.get_device_name(0)})" if device.type == "cuda" else ""))
    model = model.to(device)

    # ── Subsample training data if needed ─────────────────────────────────────
    n_seqs = len(X_train) - lookback
    if max_train_seqs is not None and n_seqs > max_train_seqs:
        keep = max_train_seqs + lookback
        X_train = X_train[-keep:]
        y_train = y_train[-keep:]
        print(f"  Subsampled training to last {keep} rows ({max_train_seqs} sequences).")

    train_ds = SequenceDataset(X_train, y_train, lookback)
    train_loader = DataLoader(
        train_ds, batch_size=batch_size, shuffle=True, num_workers=0
    )

    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, patience=3, factor=0.5, min_lr=1e-5
    )
    loss_fn = nn.MSELoss()

    val_losses: list[float] = []
    best_val_loss = float("inf")
    best_state: dict | None = None
    wait = 0

    for epoch in range(epochs):
        # ── Train ─────────────────────────────────────────────────────────────
        model.train()
        for x_batch, y_batch in train_loader:
            pred = model(x_batch.to(device))
            loss = loss_fn(pred, y_batch.to(device))
            optimizer.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

        # ── Validate ──────────────────────────────────────────────────────────
        if X_val is not None and y_val is not None:
            model.eval()
            val_ds = SequenceDataset(X_val, y_val, lookback)
            val_loader = DataLoader(val_ds, batch_size=1024, shuffle=False, num_workers=0)
            preds_v, true_v = [], []
            with torch.no_grad():
                for x_b, y_b in val_loader:
                    preds_v.append(model(x_b.to(device)).cpu())
                    true_v.append(y_b)
            val_loss = loss_fn(torch.cat(preds_v), torch.cat(true_v)).item()
            val_losses.append(val_loss)
            scheduler.step(val_loss)

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
                wait = 0
            else:
                wait += 1
                if wait >= patience:
                    print(
                        f"  Early stopping at epoch {epoch + 1}; "
                        f"best val_loss={best_val_loss:.6f}"
                    )
                    break

            if (epoch + 1) % 5 == 0 or epoch == 0:
                print(
                    f"  Epoch {epoch + 1}/{epochs}: val_loss={val_loss:.6f}  "
                    f"lr={optimizer.param_groups[0]['lr']:.2e}"
                )

    if best_state is not None:
        model.load_state_dict(best_state)

    model.eval()
    return model, val_losses


# ── Inference ─────────────────────────────────────────────────────────────────


def predict_from_sequences(
    model: nn.Module,
    X_scaled: np.ndarray,
    lookback: int,
    batch_size: int = 1024,
) -> np.ndarray:
    """
    Run inference over all sliding-window sequences in *X_scaled*.

    Args:
        model:      Trained model (eval mode will be set).
        X_scaled:   Scaled feature context of shape (lookback + N, n_features).
                    Produces N predictions (one per step from *lookback* onward).
        lookback:   Sequence length the model was trained with.
        batch_size: Inference batch size.

    Returns:
        Numpy array of shape (N,) – un-scaled (scaled) predictions.
    """
    model.eval()
    y_dummy = np.zeros(len(X_scaled), dtype=np.float32)
    ds = SequenceDataset(X_scaled.astype(np.float32), y_dummy, lookback)
    loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=0)
    preds: list[np.ndarray] = []
    with torch.no_grad():
        for x_batch, _ in loader:
            preds.append(model(x_batch).numpy())
    return np.concatenate(preds) if preds else np.array([], dtype=np.float32)


# ── Serialisation ─────────────────────────────────────────────────────────────


def save_torch_model(
    model: nn.Module,
    config: dict[str, Any],
    scaler_mean: np.ndarray,
    scaler_scale: np.ndarray,
    y_mean: float,
    y_scale: float,
    feature_cols: list[str],
    lookback: int,
    name: str,
    models_dir: str = "models/",
    val_losses: list[float] | None = None,
) -> Path:
    """
    Serialize a trained sequence model to ``<models_dir>/<name>_<YYYYMMDD>.pt``.

    The bundle contains everything needed for inference:
    state_dict, architecture config, feature scalers, target scalers,
    feature column names, and lookback window length.

    Args:
        model:        Trained nn.Module (LSTMModel or TransformerModel).
        config:       Architecture config dict (must include ``model_type``).
        scaler_mean:  Per-feature means used for z-scoring (shape: n_features).
        scaler_scale: Per-feature std devs (shape: n_features).
        y_mean:       Target mean.
        y_scale:      Target std dev.
        feature_cols: Ordered list of feature column names.
        lookback:     Sequence window length.
        name:         File stem, e.g. ``"lstm_load"``.
        models_dir:   Output directory (created if missing).
        val_losses:   Optional list of per-epoch validation MSE values.

    Returns:
        Path to the written ``.pt`` file.
    """
    out = Path(models_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = out / f"{name}_{ts}.pt"

    torch.save(
        {
            "state_dict": model.state_dict(),
            "config": config,
            "scaler_mean": scaler_mean,
            "scaler_scale": scaler_scale,
            "y_mean": y_mean,
            "y_scale": y_scale,
            "feature_cols": feature_cols,
            "lookback": lookback,
        },
        path,
    )

    if val_losses:
        loss_path = out / f"{name}_loss_{ts}.json"
        loss_path.write_text(json.dumps({"val_mse": val_losses}, indent=2))
        print(f"  Saved loss history: {loss_path}")

    return path


def load_torch_bundle(path: str | Path) -> dict:
    """
    Load a serialised torch model bundle.

    Args:
        path: Path to a ``.pt`` file written by ``save_torch_model()``.

    Returns:
        Dict with keys: state_dict, config, scaler_mean, scaler_scale,
        y_mean, y_scale, feature_cols, lookback.
    """
    return torch.load(str(path), map_location="cpu", weights_only=False)


def reconstruct_model(bundle: dict) -> nn.Module:
    """
    Rebuild an LSTMModel or TransformerModel from a saved bundle.

    Loads the state_dict and sets the model to eval mode.

    Args:
        bundle: Dict returned by ``load_torch_bundle()``.

    Returns:
        Trained model in eval mode.
    """
    cfg = bundle["config"]
    model_type = cfg["model_type"]

    if model_type == "lstm":
        model = LSTMModel(
            input_size=cfg["input_size"],
            hidden_size=cfg.get("hidden_size", 64),
            num_layers=cfg.get("num_layers", 2),
            dropout=cfg.get("dropout", 0.2),
        )
    elif model_type == "transformer":
        model = TransformerModel(
            input_size=cfg["input_size"],
            d_model=cfg.get("d_model", 64),
            nhead=cfg.get("nhead", 4),
            num_layers=cfg.get("num_layers", 2),
            dropout=cfg.get("dropout", 0.1),
        )
    else:
        raise ValueError(f"Unknown model_type in bundle: {model_type!r}")

    model.load_state_dict(bundle["state_dict"])
    model.eval()
    return model


def find_latest_torch_model(models_dir: str = "models/", prefix: str = "lstm_load") -> Path:
    """
    Return the path to the most recently created ``.pt`` file with *prefix*.

    Raises:
        FileNotFoundError: If no matching file is found.
    """
    files = sorted(Path(models_dir).glob(f"{prefix}_*.pt"))
    if not files:
        raise FileNotFoundError(
            f"No torch model with prefix {prefix!r} found in {models_dir!r}. "
            "Train a sequence model first."
        )
    return files[-1]
