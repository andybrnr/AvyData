# AvyData

## Goal

Collect avalanche forecasts and weather station data to build a ML model that
predicts real time avalanche danger.  Add a weather forecast for next 1-3
days and extend the model to do predictions for near future.


## Requirements

### Baseline
1. Timeseries of regional avalanche hazard forecasts at three levels: above treeline, treeline, and below treeline. We can obtain these by web scraping the published avalanche bulletins from [Bridger Teton Avalanche Center](http://jhavalanche.org/advisories.php), [Avalanche Canada](https://www.avalanche.ca/forecasts/archives), and the [Northwest Avalanche Center](http://www.nwac.us/)
2. Reliable weather observations in the forecast areas covering wind, air temperature, and precipitation/snowfall. Would like multiple weather stations to cover the different elevations and capture intraregional variability. It turns out there's a great interface for this through the [Synoptic API](https://synopticlabs.org/api/guides/?getstarted).

### Extended
3. Weather reanalysis dataset that will allow modeling of local stations from a lower resolution model, ideally using [MIST RNNs](https://arxiv.org/abs/1702.07805). This will allow for a continuous weather record, extending back before availability of station data and helping to infill instrument dropouts.
4. Avalanche observations to help establish ground truth and accuracy of the forecast. These may be somewhat tricky to use, as observations of natural avalanches are opportunistic and sparse, while controlled avalanches in ski areas and at highway sites has a different character, as the snowpack structure of controlled paths will not necessarily match uncontrolled areas. Datasets identified for this include series from the [Bridger Teton Avalanche Center](http://jhavalanche.org/eventmap/index.php), which is web accessible, and privately maintained datasets from Rogers Pass in Canada and the Washington State DOT.


## Tasks

### Baseline
1. Web scrapers for the regional avalanche center bulletins and miscellaneous data (complete for BTAC, TO-DO for NWAC and Avalanche Canada)
2. Scraper to collect weather data (DONE for the BTAC and NWAC, TO-DO for Canada)
3. Filtering code to clean sensor noise and dropouts from weather data (IN PROCESS)
4. RNN architecture to predict avalanche hazard
5. Create a nice web dashboard to visualize the data and forecasts

### Extended
5. RNN downscaling model to extend weather observations further back in time
6. Introduce a physical snowpack model (e.g [SNOWPACK](http://www.slf.ch/ueber/organisation/schnee_permafrost/projekte/snowpack/index_EN)) driven by the weather observations and forecasts. Predicted snowpack structure can then be directly supplied to the RNN to improve hazard forecasts.
7. Apply ensemble methods, using ensemble weather forecast models (e.g. [NAEFS](https://weather.gc.ca/ensemble/naefs/index_e.html)) to generate a range of possible weather forecasts to drive the model. Can extend this to the model itself, evaluating the forecast with multiple candidate RNN weightings to bound uncertainty.


## Challenges
1. Appropriate penalty function for forecasts: since most winter days are not avalanche days, need to handle unbalanced sampling and also account for the fact that Type 1 Errors (people stay home due to an overly conservative forecast) are more acceptable than Type 2 Errors (people die because the forecast is wrong).
2. Limited sample size and autocorrelation: there are very few "High" or "Extreme" avalanche forecast days per season, so training a Deep Neural Network to identify these conditions may be quite hard. It may be more appropriate to pose the question as a regression problem rather than a classification problem, as the scale is continuously varying (though floored at 0-"No Rating" and capped at 5-"Extreme"). Having data from as many different avalanche centers as possible is desirable here.
3. Data sparsity: weather stations are actually rather sparse in mountainous areas. Weather models have biases, and since winter precipitation has a strong non-linearity (rain/snow) around 0C, forecasts could be quite sensitive, making it hard to predict hazard in regions without a sufficient number of weather stations. Observational errors can compound over time, as avalanches exhibit very long range dependencies (layers deep within the snowpack, buried months before, can be the key ingredient for later avalanche cycles).