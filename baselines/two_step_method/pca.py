import numpy as np
from sklearn.datasets import make_friedman1
from sklearn.decomposition import SparsePCA
import csv

avg_variance = []
for k in range(1, 10):
    print(k)
    with open('benchmark/two_step/scale_0.001/sql_1/noise_2/data.csv','r') as f:
        reader = csv.reader(f)
        X = np.array(list(reader)[1:], dtype=np.int32)[:,:]

    print(X.shape)
    # X, _ = make_friedman1(n_samples=200, n_features=30, random_state=0)
    transformer = SparsePCA(n_components=k, random_state=0)
    transformer.fit(X)
    X_transformed = transformer.transform(X)
    print(X_transformed.shape)
    np.savetxt(f'benchmark/two_step/scale_0.001/sql_1/noise_2/pca_dimension_{k}.csv', transformer.components_, delimiter=',',fmt='%.4f')
    avg_variance.append(1-np.mean(transformer.components_ == 0))
print(avg_variance)