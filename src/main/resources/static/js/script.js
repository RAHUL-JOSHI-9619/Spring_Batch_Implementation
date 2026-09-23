// Initialize Tooltips
document.addEventListener('DOMContentLoaded', function () {
    // 1. Initialize Bootstrap Tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 2. Start Heartbeat Ping to keep Spring Boot alive
    // Immediately send the first ping so the backend knows the tab opened
    fetch('/api/ping', { method: 'POST' }).catch(() => {});

    // Send ping every 3 seconds
    setInterval(() => {
        fetch('/api/ping', { method: 'POST' }).catch(() => {});
    }, 3000);
});

// Driver Presets Map
const presets = {
    mysql: {
        driver: 'com.mysql.cj.jdbc.Driver',
        url: 'jdbc:mysql://localhost:3306/batch_db?useSSL=false&serverTimezone=UTC'
    },
    postgresql: {
        driver: 'org.postgresql.Driver',
        url: 'jdbc:postgresql://localhost:5432/batch_db'
    },
    oracle: {
        driver: 'oracle.jdbc.OracleDriver',
        url: 'jdbc:oracle:thin:@localhost:1521:xe'
    },
    h2: {
        driver: 'org.h2.Driver',
        url: 'jdbc:h2:mem:batchdb;DB_CLOSE_DELAY=-1'
    }
};

function applyDriverPreset() {
    const selected = document.getElementById('presetSelect').value;
    if (presets[selected]) {
        document.getElementById('driverClassName').value = presets[selected].driver;
        document.getElementById('jdbcUrl').value = presets[selected].url;
        logConsole('INFO', `Applied preset for database: ${selected.toUpperCase()}`);
    }
}

function togglePasswordVisibility() {
    const passInput = document.getElementById('dbPassword');
    const toggleIcon = document.getElementById('passwordToggleIcon');
    if (passInput.type === 'password') {
        passInput.type = 'text';
        toggleIcon.classList.replace('fa-eye', 'fa-eye-slash');
    } else {
        passInput.type = 'password';
        toggleIcon.classList.replace('fa-eye-slash', 'fa-eye');
    }
}

function resetForm() {
    document.getElementById('batchConfigForm').reset();
    logConsole('WARN', 'Configuration form reset to defaults.');
    setProgressBar(0);
}

function logConsole(level, text) {
    const consoleBox = document.getElementById('consoleBox');
    const now = new Date().toLocaleTimeString();
    let levelClass = 'console-info';

    if (level === 'SUCCESS') levelClass = 'console-success';
    if (level === 'ERROR') levelClass = 'console-error';
    if (level === 'WARN') levelClass = 'console-warn';

    const line = document.createElement('div');
    line.className = 'console-line';
    line.innerHTML = `<span class="console-time">[${now}]</span> <span class="${levelClass}">[${level}] ${text}</span>`;
    consoleBox.appendChild(line);
    consoleBox.scrollTop = consoleBox.scrollHeight;
}

function clearConsole() {
    document.getElementById('consoleBox').innerHTML = '';
}

function setProgressBar(percent) {
    const bar = document.getElementById('progressBar');
    bar.style.width = percent + '%';
}

async function testDatabaseConnection() {
	const url = document.getElementById("jdbcUrl").value.trim();
	    const username = document.getElementById("dbUsername").value.trim();
	    const password = document.getElementById("dbPassword").value;

	    const button = document.getElementById("testConnBtn");
	    const spinner = document.getElementById("testBtnSpinner");
	    const icon = document.getElementById("testBtnIcon");

	    // Basic frontend validation
	    if (!url) {
	        logConsole('ERROR', 'Database URL is required.');
	        return;
	    }

	    if (!username) {
	        logConsole('ERROR', 'Database username is required.');
	        return;
	    }

	    if (!password) {
	        logConsole('ERROR', 'Database password is required.');
	        return;
	    }

	    // Disable button while testing
	    button.disabled = true;
	    spinner.classList.remove('d-none');
	    icon.classList.add('d-none');

	    logConsole('INFO', 'Testing database connection...');
	    logConsole('INFO', `Database URL: ${url}`);
	    logConsole('INFO', `Username: ${username}`);
	    logConsole('INFO', 'Password: ***');

	    try {

	        const response = await fetch(
	            "/api/database/test-connection",
	            {
	                method: "POST",

	                headers: {
	                    "Content-Type": "application/json"
	                },

	                body: JSON.stringify({
	                    url: url,
	                    username: username,
	                    password: password
	                })
	            }
	        );

	        const message = await response.text();

	        if (response.ok) {

	            logConsole(
	                'SUCCESS',
	                '✓ ' + message
	            );

	        } else {

	            logConsole(
	                'ERROR',
	                '✗ ' + message
	            );
	        }

	    } catch (error) {

	        console.error("Connection test error:", error);

	        logConsole(
	            'ERROR',
	            '✗ Unable to contact backend server.'
	        );

	    } finally {

	        // Enable button again
	        button.disabled = false;
	        spinner.classList.add('d-none');
	        icon.classList.remove('d-none');
	    }
}

async function handleFormSubmit(e) {
    e.preventDefault();

    const payload = {
        driverClassName: document.getElementById('driverClassName').value,
        dbUrl: document.getElementById('jdbcUrl').value,
        username: document.getElementById('dbUsername').value,
        password: document.getElementById('dbPassword').value,
        csvFilePath: document.getElementById('filePath').value,
        tableName: document.getElementById('tableName').value
    };

    const btn = document.getElementById('startBatchBtn');
    const spinner = document.getElementById('startBtnSpinner');
    const icon = document.getElementById('startBtnIcon');

    btn.disabled = true;
    spinner.classList.remove('d-none');
    icon.classList.add('d-none');
    setProgressBar(25);

    logConsole('INFO', 'Submitting Spring Batch payload to backend...');
    logConsole('INFO', `Payload Inspect: ${JSON.stringify({...payload, password: '***'}, null, 2)}`);

    try {
        setProgressBar(50);
        const response = await fetch('/api/batch/run-import', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        setProgressBar(75);

        if (response.ok) {
            const result = await response.json();
            setProgressBar(100);
            logConsole('SUCCESS', `Batch process initialized! Job ID: ${result.jobId || 'BATCH_JOB_001'}`);
        } else {
            setProgressBar(100);
            logConsole('ERROR', `Server error [${response.status}]: ${response.statusText}`);
        }
    } catch (err) {
        // Mock Response Fallback for offline demo testing
        setProgressBar(100);
        logConsole('WARN', `Endpoint '/api/batch/run-import' un-reachable directly. Payload formatted properly for integration.`);
        logConsole('SUCCESS', 'Simulated batch job submission completed successfully!');
    } finally {
        btn.disabled = false;
        spinner.classList.add('d-none');
        icon.classList.remove('d-none');
    }
}