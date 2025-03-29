# Third ETL Activity

# ETL Project with Prefect

This project implements an ETL (Extract, Transform, Load) process using Python and Prefect.
The process consists of three main phases:

Extraction: Retrieves data from an SQL Server database.

Transformation: Cleans, structures, and formats the data.

Loading: Inserts the transformed data into the target database.

## Requirements

Before running the project, make sure you have installed:

Python 3.x
SQL Server DB
Prefect
Pandas
PyODBC
Numpy
OpenPyXL
Matplotlib

## Usage

1. Clone the repository:

```bash
git clone https://github.com/JahirSnake/ActividadETL-F3
cd ActividadETL-F3
```
2. Install the dependencies:

To install the required packages, you can use `pip`:

You can install the dependencies by running:

```bash
pip install -r Requirements.txt
```

3. Open your desired notebook and start working with the project.


## Project Structure

The code is organized into the following files:

ETL_prefect.py: Orchestrates the ETL process using Prefect.

extraccion.py: Contains the logic for extracting data from SQL Server.

transformacion.py: Performs data cleaning and transformation.

carga.py: Loads the processed data into the database.

Requirements.txt: List of required dependencies.

## Configuration

In ETL_prefect.py, define the database connection parameters:

```bash
SERVER = "127.0.0.1"
DATABASE = "actividadETL"
USERNAME = "sa"
PASSWORD = "*********"
```

Make sure to modify these values according to your setup.

Execution

To run the ETL process, simply execute:

```bash
python ETL_prefect.py
```

This will run the complete extract, transform, and load process.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or suggestions, please contact [jahirsnake@hotmail.com].