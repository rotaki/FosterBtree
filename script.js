// Import the init function and the module class
import init, { FosterBtreeVisualizer } from './pkg/fbtree.js';

// First, ensure the module is initialized before using it
init().then(() => {
    // Module is initialized, now you can create an instance of FosterBtreeVisualizer
    const fosterTree = new FosterBtreeVisualizer();

    // Set up event listeners for insert and get actions
    setupInsert(fosterTree);
    setupUpdate(fosterTree);
    setupDelete(fosterTree);
    setupUpsert(fosterTree);
    setupGet(fosterTree);
    setupClearLogs();
}).catch(error => {
    console.error('Error initializing the WebAssembly module:', error);
});

function setupInsert(fosterTree) {
    const insertButton = document.getElementById('insertButton');
    insertButton.addEventListener('click', () => {
        const key = document.getElementById('insertKey').value;
        const payloadSize = document.getElementById('payloadSize').value;
        const result = fosterTree.insert(key, payloadSize);
        const message = `Insert operation performed on key: ${key} with payload size: ${payloadSize}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function setupUpdate(fosterTree) {
    const updateButton = document.getElementById('updateButton');
    updateButton.addEventListener('click', () => {
        const key = document.getElementById('updateKey').value;
        const payloadSize = document.getElementById('newPayloadSize').value;
        const result = fosterTree.update(key, payloadSize);
        const message = `Update operation performed on key: ${key} with payload size: ${payloadSize}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function setupDelete(fosterTree) {
    const deleteButton = document.getElementById('deleteButton');
    deleteButton.addEventListener('click', () => {
        const key = document.getElementById('deleteKey').value;
        const result = fosterTree.delete(key);
        const message = `Delete operation performed on key: ${key}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function setupUpsert(fosterTree) {
    const upsertButton = document.getElementById('upsertButton');
    upsertButton.addEventListener('click', () => {
        const key = document.getElementById('upsertKey').value;
        const payloadSize = document.getElementById('upsertPayloadSize').value;
        const result = fosterTree.upsert(key, payloadSize);
        const message = `Upsert operation performed on key: ${key} with payload size: ${payloadSize}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function setupGet(fosterTree) {
    const getButton = document.getElementById('getButton');
    getButton.addEventListener('click', () => {
        const key = document.getElementById('getKey').value;
        const result = fosterTree.get(key);
        const message = `get operation performed on key: ${key}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function logOperation(fosterTree, message) {
    console.log(message); // Print to console log

    // Append the operation message to the container
    const container = document.getElementById('contentContainer');
    const logEntry = document.createElement('div');
    logEntry.textContent = message;
    container.appendChild(logEntry);

    // Visualize the tree and append the graph
    const dotString = fosterTree.visualize(); // Assuming visualize returns a DOT representation of the tree
    const graphContainer = document.createElement('div');
    container.appendChild(graphContainer);
    d3.select(graphContainer).graphviz().renderDot(dotString);
}

function setupClearLogs() {
    const clearLogsButton = document.getElementById('clearLogsButton');
    clearLogsButton.addEventListener('click', () => {
        const container = document.getElementById('contentContainer');
        if (container.children.length > 2) {
            const secondToLastElement = container.children[container.children.length - 2];
            const lastElement = container.lastElementChild;
            container.innerHTML = ''; // Clear all children
            container.appendChild(secondToLastElement); // Re-append the second to last child
            container.appendChild(lastElement); // Re-append the last child
        }
    });
}
