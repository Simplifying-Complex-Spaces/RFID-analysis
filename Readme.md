# Classifying the Position of a Person Based on RFID Sensor Data and SVMs  
This repository contains the setup to analyze RFID sensor data using a support vector machines (SVM). With data maintained in a SQLite database, the analysis in a RMarkdown file, and the entire ecosystem within a Docker container, everyone can verify the results with the exact same tools. Even better, everyone can extend the analysis for their own purposes with the exact same data. 

## Setup  
-   Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/) for the Docker engine and Docker Compose; Docker Desktop is [free](https://www.docker.com/pricing/) for personal use\
-   Clone (with a git client) or download this repository
-   Make sure `Docker Desktop` is running on your system
-   Open a terminal / command line and navigate to where you cloned the directory
-   Start up the analysis environment:

``` zsh
docker compose -f compose-scs-rstudio.yml up
```

-   Navigate to the local web interface for the application: [localhost:7320](http://localhost:7320)
-   In the lower right files panel, click `notebooks` ![fig1](./images/notebooks_ref.png)
-   Next, click `R_Notebooks` ![fig2](./images/R_notebooks_ref.png)
-   Then, click `svm_analysis.Rmd` to open the RNotebook ![fig3](./images/rmd_ref.png)
-   Finally, re-run all the analysis and generate a fresh HTML report ![fig4](./images/knit_html.png)

## Shutdown  
-   Open a new command line / terminal and navigate to where you cloned the directory
-   Shutdown the analysis environment:

``` zsh
docker compose -f compose-scs-rstudio.yml down 
```
