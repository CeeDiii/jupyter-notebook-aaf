<p align="center">
    <h1 align="center">JUPYTER-NOTEBOOK-AAF</h1>
</p>
<p align="center">
    <em>Jupyter As Functions: Execute, Automate, Innovate</em>
</p>
<p align="center">
	<img src="https://img.shields.io/github/license/Dolder-AG/jupyter-notebook-aaf?style=default&color=0080ff" alt="license">
	<img src="https://img.shields.io/github/last-commit/Dolder-AG/jupyter-notebook-aaf?style=default&color=0080ff" alt="last-commit">
	<img src="https://img.shields.io/github/languages/top/Dolder-AG/jupyter-notebook-aaf?style=default&color=0080ff" alt="repo-top-language">
	<img src="https://img.shields.io/github/languages/count/Dolder-AG/jupyter-notebook-aaf?style=default&color=0080ff" alt="repo-language-count">
<p>
<p align="center">
	<!-- default option, no dependency badges. -->
</p>
<hr>

## Quick Links

> -   [ Overview](#-overview)
> -   [ Features](#-features)
> -   [ Modules](#-modules)
> -   [ Getting Started](#-getting-started)
>     -   [ Installation](#-installation)
>     -   [ Running jupyter-notebook-aaf](#-running-jupyter-notebook-aaf)
> -   [ Contributing](#-contributing)
> -   [ License](#-license)
> -   [ Acknowledgments](#-acknowledgments)

---

## Overview

The Jupyter Notebook AAF project provides a serverless solution for executing Jupyter notebooks in Azure functions, allowing users to run computationally intensive tasks on demand. The codebase implements an HTTP-triggered function that orchestrates the execution of notebooks stored in Azure Blob Storage, leveraging the Papermill library for parameterized and executable notebooks. It proposes an infrastructure to execute notebooks as functions, enabling the integration of data analysis workflows with Azure's scalable compute resources. The project encapsulates notebook execution parameters, Papermill execution outputs, and provides the capability to extract specific results, offering a flexible and automated way to perform data analysis tasks in the cloud.

---

## Getting Started

**_Requirements_**

Ensure you have the following dependencies installed on your system:

-   **Python**: `version >3.10.X`

### Installation

1. Clone the jupyter-notebook-aaf repository:

```sh
git clone https://github.com/Dolder-AG/jupyter-notebook-aaf
```

2. Change to the project directory:

```sh
cd jupyter-notebook-aaf
```

3. Install the dependencies:

```sh
pip install -r requirements.txt
```

4. Create `local.settings.json` for env variables:

```json
{
    "IsEncrypted": false,
    "Values": {
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
        "BLOB_CONNECTION_STRING": "YOUR_BLOB_CONNECTION_STRING"
    }
}
```

### Running jupyter-notebook-aaf locally

Start the `azurite` storage emulator:

```sh
azurite -l .azurite
```

Use the following command to run jupyter-notebook-aaf:

```sh
func host start
```

---

## Contributing

Contributions are welcome! Here are several ways you can contribute:

-   **[Submit Pull Requests](https://github/Dolder-AG/jupyter-notebook-aaf/blob/main/CONTRIBUTING.md)**: Review open PRs, and submit your own PRs.
-   **[Join the Discussions](https://github/Dolder-AG/jupyter-notebook-aaf/discussions)**: Share your insights, provide feedback, or ask questions.
-   **[Report Issues](https://github/Dolder-AG/jupyter-notebook-aaf/issues)**: Submit bugs found or log feature requests for Jupyter-notebook-aaf.

<details closed>
    <summary>Contributing Guidelines</summary>

1. **Fork the Repository**: Start by forking the project repository to your GitHub account.
2. **Clone Locally**: Clone the forked repository to your local machine using a Git client.
    ```sh
    git clone https://github.com/Dolder-AG/jupyter-notebook-aaf
    ```
3. **Create a New Branch**: Always work on a new branch, giving it a descriptive name.
    ```sh
    git checkout -b new-feature-x
    ```
4. **Make Your Changes**: Develop and test your changes locally.
5. **Commit Your Changes**: Commit with a clear message describing your updates.
    ```sh
    git commit -m 'Implemented new feature x.'
    ```
6. **Push to GitHub**: Push the changes to your forked repository.
    ```sh
    git push origin new-feature-x
    ```
7. **Submit a Pull Request**: Create a PR against the original project repository. Clearly describe the changes and their motivations.

Once your PR is reviewed and approved, it will be merged into the main branch.

</details>

---

## License

[MIT License](./LICENSE.TXT)

---

## Acknowledgments

-   List any resources, contributors, inspiration, etc. here.

[**Return**](#-quick-links)

---
