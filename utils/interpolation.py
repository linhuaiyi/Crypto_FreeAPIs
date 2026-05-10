"""Cubic spline interpolation for yield curve construction."""
from typing import List, Tuple

import numpy as np
from scipy.interpolate import CubicSpline


def interpolate_curve(
    known_points: List[Tuple[float, float]],
    target_x: List[float],
    extrapolate: str = "const",
) -> List[float]:
    """Interpolate/extrapolate y-values at target_x positions.

    Args:
        known_points: [(x, y), ...] known data points, sorted by x
        target_x: x positions to interpolate at
        extrapolate: 'const' holds edge values, 'linear' extends linearly

    Returns:
        Interpolated y values at each target_x
    """
    if not known_points:
        return [0.0] * len(target_x)

    xs = np.array([p[0] for p in known_points])
    ys = np.array([p[1] for p in known_points])

    cs = CubicSpline(xs, ys, bc_type="natural")

    result: List[float] = []
    for x in target_x:
        if x <= xs[0]:
            result.append(float(ys[0]) if extrapolate == "const" else float(cs(x)))
        elif x >= xs[-1]:
            result.append(float(ys[-1]) if extrapolate == "const" else float(cs(x)))
        else:
            result.append(float(cs(x)))

    return result
