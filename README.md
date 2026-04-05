# Course Project: Options Data Pipeline Development and Visualization

In this course project, we implemented

## Point-selection model

Based on the required expiries and strikes, the model selects the desired data points from the raw option data for each quote date according to the minimum-distance principle. 

Details can be found in the pick_points.sql file.

## Arbitrage check

This part computes quantities that are not directly provided in the raw data, such as implied volatility, delta, relative spread, and 25-delta skew.

It also includes a series of checks for abnormal data points, such as extreme IV, extreme spread, local IV outliers, spread-liquidity outliers, and skew jumps.

Details can be found in Other-quantity.cpp and kpi.cpp.

## Data visualization

Tableau is used to present various quantitative relationships, such as the volatility smile/skew and the relationship between relative spread and volume.

More details can be found in Options Data Visualization.twb in the results folder.
