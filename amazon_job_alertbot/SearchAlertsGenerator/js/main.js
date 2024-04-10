document.addEventListener("DOMContentLoaded", function () {
  const searchBar = new Choices(document.getElementById("search-bar"), {
    allowHTML: true,
    delimiter: " ",
    editItems: true,
    paste: true,
    searchEnabled: false,
    maxItemCount: 5,
    maxItemText: "You may only add 5 search phrases",
    searchChoices: false,
    placeholder: true,
    placeholderValue: "enter your search terms here",
  });

  const busCats = new Choices("#business_category", {
    allowHTML: true,
    removeItemButton: true,
    duplicateItemsAllowed: false,
    editItems: false,
    maxItemCount: 5,
    searchEnabled: true,
    itemSelectText: "",
    paste: false,
  });

  const catCats = new Choices("#category", {
    allowHTML: true,
    removeItemButton: true,
    duplicateItemsAllowed: false,
    editItems: false,
    maxItemCount: 5,
    searchEnabled: true,
    itemSelectText: "",
    paste: false,
  });

  const catTypes = new Choices("#category_type", {
    allowHTML: true,
    removeItemButton: true,
    duplicateItemsAllowed: false,
    editItems: false,
    maxItemCount: 5,
    searchEnabled: false,
    itemSelectText: "",
    paste: false,
  });

  const jobTypes = new Choices("#job_type", {
    allowHTML: true,
    removeItemButton: true,
    duplicateItemsAllowed: false,
    editItems: false,
    maxItemCount: 5,
    searchEnabled: false,
    paste: false,
    itemSelectText: "",
  });

  const isManager = new Choices("#is_manager", {
    allowHTML: true,
    duplicateItemsAllowed: false,
    editItems: false,
    searchEnabled: false,
    paste: false,
    itemSelectText: "",
  });

  const countriesSelect = new Choices("#country", {
    allowHTML: true,
    duplicateItemsAllowed: false,
    editItems: true,
    searchEnabled: true,
    itemSelectText: "",
    paste: true,
  });

  const statesSelect = new Choices("#state", {
    allowHTML: true,
    duplicateItemsAllowed: false,
    editItems: true,
    searchEnabled: true,
    itemSelectText: "",
    paste: true,
  });

  const citiesSelect = new Choices("#cities", {
    allowHTML: true,
    duplicateItemsAllowed: false,
    editItems: true,
    searchEnabled: true,
    itemSelectText: "",
    paste: true,
  });

  const alertFrequency = new Choices("#alert_frequency", {
    allowHTML: true,
    duplicateItemsAllowed: false,
    editItems: false,
    searchEnabled: false,
    itemSelectText: "",
    paste: false,
  });

  const textEmailFilter = new Choices("#youremail", {
    allowHTML: true,
    editItems: true,
    placeholderValue: "Enter your email to receive alerts",
    placeholder: true,
    addItemFilter: function (value) {
      if (!value) {
        return false;
      }

      const regex =
        /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
      const expression = new RegExp(regex.source, "i");
      return expression.test(value);
    },
  });

  const langCode = new Choices("#lang_code", {
    allowHTML: true,
    duplicateItemsAllowed: false,
    editItems: false,
    searchEnabled: false,
    itemSelectText: "",
    paste: false,
  });

  const resetButton = document.getElementById("btn-delete");

  const itemsToRemoveActiveFrom = [
    searchBar,
    busCats,
    catCats,
    catTypes,
    jobTypes,
    isManager,
    countriesSelect,
    statesSelect,
    citiesSelect,
    alertFrequency,
    textEmailFilter,
    langCode,
  ];

  const confirmAndReset = function () {
    if (confirm("Are you sure you want to reset all fields?")) {
      itemsToRemoveActiveFrom.forEach((item) => {
        item.removeActiveItems();
      });
    }
  };

  resetButton.addEventListener("click", confirmAndReset);

  function setUpdateListener() {
    const change = ".1rem solid #6B7583";
    const alertDiv = document.getElementById("alerts_grid");
    const choices = alertDiv.getElementsByClassName("choices__inner");
    for (let i = 0; i < choices.length; i++) {
      choices[i].addEventListener("change", function () {
        changeBorderForRequired(change);
      });
    }
  }

  function changeBorderForRequired(borderStyle) {
    const targetDiv = document.getElementById("alerts_grid");
    const elements = targetDiv.getElementsByClassName("choices__inner");
    for (let i = 0; i < elements.length; i++) {
      elements[i].style.border = borderStyle;
    }
  }
  const submitButton = document.getElementById("btn-submit");

  const checkRequiredFields = () => {
    return (
      alertFrequency.getValue() &&
      textEmailFilter.getValue() &&
      langCode.getValue()
    );
  };

  const getValueOrEmpty = (selector) => {
    const value = selector.getValue();
    return value ? value.value || value.map((option) => option.value) : "";
  };

  const createSelectionsObject = () => {
    return {
      base_query: getValueOrEmpty(searchBar),
      business_category: getValueOrEmpty(busCats),
      category: getValueOrEmpty(catCats),
      category_type: getValueOrEmpty(catTypes),
      job_type: getValueOrEmpty(jobTypes),
      is_manager: getValueOrEmpty(isManager),
      country: getValueOrEmpty(countriesSelect),
      region: getValueOrEmpty(statesSelect),
      city: getValueOrEmpty(citiesSelect),
      alert_freq: getValueOrEmpty(alertFrequency),
      youremail: textEmailFilter.getValue() || "",
      lang_code: getValueOrEmpty(langCode),
    };
  };

  const displayJsonOutput = (selections) => {
    const jsonBlock = document.getElementById("jsonOutput");
    jsonBlock.value = JSON.stringify(selections, null, 2);
    document.getElementById("outputsection").style.display = "block";
    jsonBlock.scrollIntoView({ behavior: "smooth" });
  };

  submitButton.addEventListener("click", function (event) {
    if (!checkRequiredFields()) {
      alert("Please fill in all required fields.");
      event.preventDefault();
      return;
    }

    const selections = createSelectionsObject();
    displayJsonOutput(selections);
  });

  const copyButton = document.getElementById("copyButton");

  const copyToClipboard = async (elementId) => {
    const copyText = document.getElementById(elementId);
    try {
      await navigator.clipboard.writeText(copyText.value);
      copyButton.innerHTML = "Copied!";
    } catch (err) {
      console.error("Failed to copy: ", err);
      // Optionally handle the error case (e.g., show an error message)
    }
  };

  copyButton.addEventListener("click", () => copyToClipboard("jsonOutput"));

  const download = function (data, filename, type) {
    let file = new Blob([data], { type: type });
    if (window.navigator.msSaveOrOpenBlob) {
      window.navigator.msSaveOrOpenBlob(file, filename);
    } else {
      const a = document.createElement("a"),
        url = URL.createObjectURL(file);
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      setTimeout(() => {
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
      }, 0);
    }
  };

  const formatDateForFilename = () => {
    return new Date().toISOString().slice(0, 19).replace(/[-:]/g, "");
  };

  const saveButton = document.getElementById("saveButton");
  saveButton.addEventListener("click", () => {
    const text = document.getElementById("jsonOutput").value;
    const filename = `job_alert_event${formatDateForFilename()}.json`;
    download(text, filename, "text/plain");
  });
});
