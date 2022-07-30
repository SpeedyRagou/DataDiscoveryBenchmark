# Data Discovery Benchmark

You can setup this project on Linux, Windows, or MacOS. Additionally, you can also use Google Colab.

Here is an example about using the project in Google Colab: [Feature Augmentation with Webtables](https://colab.research.google.com/drive/1wgJXLl8rnsxtCBcWWPugqlxfw6CQpVg_?usp=sharing)


This is how to setup the project on your machine:
```
conda create -n Bench python=3.8
conda activate Bench
git clone https://github.com/LUH-DBS/DataDiscoveryBenchmark.git
cd DataDiscoveryBenchmark
git pull origin main
python -m pip install .
```

If `python -m pip install .` does not work, try this command:
```
python -m pip install --use-feature=in-tree-build .
```

You can run the feature augmentation task with:
```
python datadiscoverybench/feature_augmentation/augmentation.py
```

On Mac OS, you need to run the following command additionally:
```
brew install libomp
```