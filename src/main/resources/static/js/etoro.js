async function buttonEtoroCargarDesdeExcel() {
    const confirmed = confirm("Are you sure you want to recalculate the data? This action cannot be undone.");
    if (confirmed) {
try {
    const response = await fetch('/api/insertFromExcel', {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
//      body: JSON.stringify("datos")
    });

    if (!response.ok) {
      throw new Error('Failed to insert data from Excel');
    }

//    const responseData = await response.json();
    const responseData = await response.text();
    alert(responseData);
    window.location.href = 'resultado1ok.html';
  } catch (error) {
    alert('Error: ' + error.message);
  }
}
}

async function buttonEtoroTratarDatos() {
    const confirmed = confirm("Are you sure you want to recalculate the data? This action cannot be undone.");
    if (confirmed) {
try {
    const response = await fetch('/api/runGlobalProcess', {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
    });

    if (!response.ok) {
      throw new Error('Failed running Main process');
    }

    const responseData = await response.text();
    alert(responseData);
    window.location.href = 'resultado1ok.html';
  } catch (error) {
    alert('Error: ' + error.message);
  }
}
}
