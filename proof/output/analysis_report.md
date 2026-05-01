# Rolling-Window Proof Report

## Setup

- Residual regression window: `252` trading days
- Specific-volatility window: `20` trading days
- Static benchmark: one full-sample beta per stock, then residual standard deviation
- Rolling method: daily rolling beta, then residual standard deviation

## Experiment 1: Residual purity

Goal: test whether residuals still retain exposure to the four pricing factors.

```
 method  stocks  median_r2  mean_r2   p90_r2  median_coef_l1  median_abs_mkt  median_abs_smb  median_abs_hml  median_abs_umd
rolling   13460   0.004231 0.007745 0.015389        0.302349        0.040944        0.074338        0.089179        0.060293
 static   13460   0.015586 0.023720 0.052583        0.603355        0.079357        0.146553        0.198903        0.100241
```

Subperiod view:
```
      period  method  stocks  median_r2  mean_r2   p90_r2  median_coef_l1  median_abs_mkt  median_abs_smb  median_abs_hml  median_abs_umd
   2017-2019 rolling    3561   0.005578 0.009701 0.020746        0.395358        0.045877        0.109380        0.100724        0.091398
   2017-2019  static    3561   0.021915 0.029911 0.063404        0.831396        0.080905        0.247328        0.259430        0.146230
   2020-2022 rolling    4492   0.004722 0.009299 0.019693        0.322237        0.042607        0.093892        0.092060        0.057755
   2020-2022  static    4492   0.014228 0.022410 0.048750        0.586200        0.081822        0.184610        0.160275        0.105905
2023-2026YTD rolling    5407   0.003316 0.005166 0.009733        0.241902        0.036889        0.052320        0.079407        0.048620
2023-2026YTD  static    5407   0.012933 0.020730 0.047947        0.508672        0.076117        0.090538        0.203806        0.077505
```

- Rolling residuals have lower median `R^2` than static residuals.
- Rolling residuals have lower median factor-loading L1 norm than static residuals.
- If both are lower, that is direct evidence that rolling windows strip factor exposure more cleanly.

## Experiment 2: Beta drift over time

Goal: verify that stock betas are not constant through time.

Representative stocks:
```
  ts_code      name  mv_quantile     total_mv
300716.SZ 300716.SZ         0.05  214906.8600
300537.SZ 300537.SZ         0.35  489799.9700
301209.SZ 301209.SZ         0.65 1079680.0000
002156.SZ 002156.SZ         0.95 7419531.3028
```

Rolling-beta summary:
```
factor  mean_std_beta  mean_range_beta
   HML       0.455329         2.156809
   MKT       0.222512         0.994300
   SMB       0.415803         1.851849
   UMD       0.333438         1.516455
```

- Large within-stock beta standard deviation or range means a fixed full-sample beta is misspecified.
- See the per-stock figures in `proof/output/figures/experiment2_beta_path_*.png`.

## Experiment 3: Subperiod stability

Goal: compare static and rolling `spec_vol` across market regimes.

```
    obs  daily_points   ic_mean     ic_ir  spread_ann_ret  spread_sharpe sample       period  method
2323516           685 -0.059650 -0.508043        0.759049       3.848787    全市场    2017-2019  static
2064514           685 -0.060710 -0.536750        0.669989       3.719446    全市场    2017-2019 rolling
3197539           727 -0.059108 -0.444103        0.432125       1.942275    全市场    2020-2022  static
2883115           727 -0.059479 -0.445614        0.426813       1.933485    全市场    2020-2022 rolling
4227639           797 -0.068286 -0.502400        0.325265       1.404498    全市场 2023-2026YTD  static
4067324           797 -0.067599 -0.487665        0.315911       1.391928    全市场 2023-2026YTD rolling
 412225           685 -0.025723 -0.156694        0.015787       0.177252  中证800    2017-2019  static
 380616           685 -0.026125 -0.160511        0.001090       0.098475  中证800    2017-2019 rolling
 524064           727 -0.026696 -0.138679       -0.175659      -0.606217  中证800    2020-2022  static
 494919           727 -0.027686 -0.144484       -0.175333      -0.634821  中证800    2020-2022 rolling
 629938           797 -0.044331 -0.247392       -0.143752      -0.506493  中证800 2023-2026YTD  static
 620133           797 -0.045009 -0.246403       -0.154840      -0.561498  中证800 2023-2026YTD rolling
 455372           685 -0.048494 -0.351625        0.364961       1.850721 中证1000    2017-2019  static
 407882           685 -0.049573 -0.369781        0.338447       1.862975 中证1000    2017-2019 rolling
 602957           727 -0.047757 -0.297894        0.105051       0.543747 中证1000    2020-2022  static
 560035           727 -0.048747 -0.306795        0.063680       0.381727 中证1000    2020-2022 rolling
 775761           797 -0.059468 -0.354804        0.106137       0.548554 中证1000 2023-2026YTD  static
 749988           797 -0.059007 -0.342482        0.072538       0.411751 中证1000 2023-2026YTD rolling
```

Stability summary (lower standard deviation across periods is better):
```
sample  method   mean_ic   std_ic   mean_ir   std_ir  mean_spread_ann  std_spread_ann  mean_spread_sharpe  std_spread_sharpe
中证1000 rolling -0.052442 0.005700 -0.339686 0.031586         0.158222        0.156142            0.885484           0.846665
中证1000  static -0.051906 0.006559 -0.334774 0.031978         0.192050        0.149746            0.981007           0.753198
 中证800 rolling -0.032940 0.010481 -0.183799 0.054805        -0.109694        0.096488           -0.365948           0.403869
 中证800  static -0.032250 0.010474 -0.180922 0.058266        -0.101208        0.102569           -0.311819           0.426473
   全市场 rolling -0.062596 0.004376 -0.490010 0.045613         0.470905        0.181110            2.348286           1.217941
   全市场  static -0.062348 0.005150 -0.484849 0.035399         0.505480        0.226004            2.398520           1.284428
```

Selected stability pivot:
```
        std_ic         std_spread_sharpe
method rolling  static           rolling  static
sample
中证1000  0.0057  0.0066            0.8467  0.7532
中证800   0.0105  0.0105            0.4039  0.4265
全市场     0.0044  0.0051            1.2179  1.2844
```

## Factor-value shift

This is not the core proof, but it shows whether rolling materially changes the factor values themselves.

```
 aligned_rows  pearson  spearman  daily_pearson_mean  daily_spearman_mean
      9020548 0.992741  0.987158            0.992135             0.985599
```

- High correlation means the signal direction is broadly preserved.
- Lower-than-1 correlation means rolling windows are materially re-ordering cross-sectional stock rankings.

## Reading guide

- Best direct proof: Experiment 1. If rolling residuals have lower residual-on-factor `R^2` and lower residual factor-loadings, then rolling windows are more correct by construction.
- Best intuitive proof: Experiment 2. If betas drift a lot, using one full-sample beta is internally inconsistent.
- Best practical proof: Experiment 3. If rolling windows keep signal quality more stable across subperiods, they are more robust for research and live use.
