import requests
import dlt


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


def fetch_pages():
    page = 1

    while True:
        response = requests.get(
            BASE_URL,
            params={"page": page}
        )

        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"No more data at page {page}. Stopping.")
            break

        print(f"Loaded page {page} with {len(data)} records")
        yield data

        page += 1


@dlt.resource(name="yellow_taxi_data")
def taxi_data():
    yield from fetch_pages()


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi"
    )

    load_info = pipeline.run(taxi_data())
    print(load_info)


if __name__ == "__main__":
    run_pipeline()