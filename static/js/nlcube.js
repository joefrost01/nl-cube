import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@3.3.0/dist/cdn/perspective-viewer.js";
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@3.3.0/dist/cdn/perspective-viewer-datagrid.js";
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@3.3.0/dist/cdn/perspective-viewer-d3fc.js";

import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective@3.3.0/dist/cdn/perspective.js";
console.log("✅ hydrocube.js is loaded and executing!");

// Run immediately if DOM is already loaded
if (document.readyState === "loading") {
    console.log("📌 Adding DOMContentLoaded listener...");
    document.addEventListener("DOMContentLoaded", initHydroCube);
} else {
    console.log("📌 DOM already loaded, running immediately...");
    initHydroCube();
}


async function initHydroCube() {

    // ----- Perspective Viewer Setup -----
    const viewer = document.querySelector("perspective-viewer");
    if (!viewer) {
        console.error("❌ Perspective Viewer element not found!");
        return;
    }
    console.log("✅ Found Perspective Viewer:", viewer);

    try {
        console.log("🔧 Initializing Perspective Worker...");

        // **FIXED: Using a Perspective Worker just like the official example**
        const worker = await perspective.worker();

        console.log("📌 Creating Table via Worker...");
        const table = await worker.table(
            { category: "string", value: "float" },
            { index: "category" } // Optional: specify an index column
        );

        console.log("📌 Populating Table...");
        await table.update([
            { category: "A", value: 10 },
            { category: "B", value: 20 },
            { category: "C", value: 30 }
        ]);

        console.log("📌 Attaching Table to Viewer...");
        await viewer.load(table);
        viewer.setAttribute("plugin", "datagrid");
        viewer.setAttribute("columns", '["category", "value"]');
        viewer.setAttribute("theme", "Pro Dark");

        console.log("🎉 Perspective Viewer Initialized!");

    } catch (error) {
        console.error("❌ Error Initializing Perspective:", error);
    }

    // Load the table **only when Perspective is ready**
    viewer.addEventListener("perspective-ready", async () => {
        try {
            await viewer.load(table);
            viewer.setAttribute("plugin", "datagrid");
            viewer.setAttribute("columns", '["category", "value"]'); // Make sure columns are displayed
            console.log("Perspective initialized with table data.");
        } catch (err) {
            console.error("Error loading data into Perspective:", err);
        }
    });

    // ----- Datasets & Reports (Dummy Data) -----
    async function fetchDatasets() {
        return [
            { name: "Dataset 1", id: 1 },
            { name: "Dataset 2", id: 2 }
        ];
    }

    async function fetchReports() {
        return {
            "Sales": [
                { name: "Q1 Sales", id: "s1" },
                { name: "Q2 Sales", id: "s2" }
            ],
            "Marketing": [
                { name: "Campaign A", id: "m1" },
                { name: "Campaign B", id: "m2" }
            ]
        };
    }

    async function populateDatasets() {
        const datasets = await fetchDatasets();
        const datasetsMenu = document.getElementById("datasetsDropdownMenu");
        datasetsMenu.innerHTML = "";
        datasets.forEach(dataset => {
            const li = document.createElement("li");
            const a = document.createElement("a");
            a.className = "dropdown-item";
            a.href = "#"; // TODO: implement dataset selection
            a.textContent = dataset.name;
            li.appendChild(a);
            datasetsMenu.appendChild(li);
        });
    }

    async function populateReports() {
        const reportsByCategory = await fetchReports();
        const reportsMenu = document.getElementById("reportsDropdownMenu");
        reportsMenu.innerHTML = "";
        for (const category in reportsByCategory) {
            const header = document.createElement("h6");
            header.className = "dropdown-header";
            header.textContent = category;
            reportsMenu.appendChild(header);

            reportsByCategory[category].forEach(report => {
                const li = document.createElement("li");
                const a = document.createElement("a");
                a.className = "dropdown-item";
                a.href = "#"; // TODO: implement report loading
                a.textContent = report.name;
                li.appendChild(a);
                reportsMenu.appendChild(li);
            });

            const divider = document.createElement("li");
            divider.innerHTML = '<hr class="dropdown-divider">';
            reportsMenu.appendChild(divider);
        }
        if (reportsMenu.lastChild) {
            reportsMenu.removeChild(reportsMenu.lastChild);
        }
    }

    populateDatasets();
    populateReports();

    // ----- Save Report Modal & Form Handling -----
    const saveReportButton = document.getElementById("saveReportButton");
    const saveReportModalElement = document.getElementById("saveReportModal");
    const saveReportModal = new bootstrap.Modal(saveReportModalElement);

    saveReportButton.addEventListener("click", function () {
        saveReportModal.show();
    });

    const saveReportForm = document.getElementById("saveReportForm");
    saveReportForm.addEventListener("submit", async function (e) {
        e.preventDefault();
        const reportName = document.getElementById("reportName").value.trim();
        const reportCategory = document.getElementById("reportCategory").value.trim();
        if (!reportName || !reportCategory) {
            alert("Please fill in both report name and category.");
            return;
        }
        const reportState = await viewer.save();
        const reportData = {
            name: reportName,
            category: reportCategory,
            state: reportState
        };
        console.log("Saving report:", reportData);
        saveReportModal.hide();
        populateReports();
    });
}
