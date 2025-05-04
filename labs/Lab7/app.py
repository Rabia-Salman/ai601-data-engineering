import streamlit as st
import joblib
import numpy as np
import pandas as pd

model = joblib.load("iris_model.pkl")

species = {0: "setosa", 1: "versicolor", 2: "virginica"}

st.title("Iris Species Classifier")
st.write("Enter the measurements of an Iris flower to predict its species.")

sepal_length = st.number_input("Sepal Length (cm)", min_value=0.0, value=5.1)
sepal_width  = st.number_input("Sepal Width (cm)", min_value=0.0, value=3.5)
petal_length = st.number_input("Petal Length (cm)", min_value=0.0, value=1.4)
petal_width  = st.number_input("Petal Width (cm)", min_value=0.0, value=0.2)

if st.button("Predict"):
    input_features = np.array([[sepal_length, sepal_width, petal_length, petal_width]])
    prediction = model.predict(input_features)
    st.success(f"The predicted Iris species is **{species[prediction[0]]}**.")



feature_names = ["Sepal Length", "Sepal Width", "Petal Length", "Petal Width"]
input_values = [sepal_length, sepal_width, petal_length, petal_width]
df = pd.DataFrame(data={'Measurement (cm)': input_values}, index=feature_names)
st.subheader("Input Feature Measurements")
st.bar_chart(df)

