// Lấy các phần tử DOM
const taskInput = document.getElementById('taskInput');
const addBtn = document.getElementById('addBtn');
const taskList = document.getElementById('taskList');
const taskCount = document.getElementById('taskCount');
const clearCompleted = document.getElementById('clearCompleted');

// Mảng lưu trữ công việc
let tasks = JSON.parse(localStorage.getItem('tasks')) || [];

// Khởi tạo hiển thị
renderTasks();

// Thêm công việc mới
function addTask() {
    const text = taskInput.value.trim();
    if (text === '') return;

    const task = {
        id: Date.now(),
        text: text,
        completed: false
    };

    tasks.push(task);
    saveTasks();
    renderTasks();
    taskInput.value = '';
    taskInput.focus();
}

// Xóa công việc
function deleteTask(id) {
    tasks = tasks.filter(task => task.id !== id);
    saveTasks();
    renderTasks();
}

// Đánh dấu hoàn thành
function toggleTask(id) {
    tasks = tasks.map(task => {
        if (task.id === id) {
            return { ...task, completed: !task.completed };
        }
        return task;
    });
    saveTasks();
    renderTasks();
}

// Xóa tất cả công việc đã hoàn thành
function clearCompletedTasks() {
    tasks = tasks.filter(task => !task.completed);
    saveTasks();
    renderTasks();
}

// Lưu vào localStorage
function saveTasks() {
    localStorage.setItem('tasks', JSON.stringify(tasks));
}

// Hiển thị danh sách công việc
function renderTasks() {
    taskList.innerHTML = '';

    tasks.forEach(task => {
        const li = document.createElement('li');
        li.className = `task-item ${task.completed ? 'completed' : ''}`;

        li.innerHTML = `
            <input type="checkbox" class="task-checkbox" ${task.completed ? 'checked' : ''}>
            <span class="task-text">${escapeHtml(task.text)}</span>
            <button class="delete-btn">Xóa</button>
        `;

        // Sự kiện checkbox
        const checkbox = li.querySelector('.task-checkbox');
        checkbox.addEventListener('change', () => toggleTask(task.id));

        // Sự kiện xóa
        const deleteBtn = li.querySelector('.delete-btn');
        deleteBtn.addEventListener('click', () => deleteTask(task.id));

        taskList.appendChild(li);
    });

    updateTaskCount();
}

// Cập nhật số lượng công việc
function updateTaskCount() {
    const total = tasks.length;
    const completed = tasks.filter(task => task.completed).length;
    taskCount.textContent = `${completed}/${total} hoàn thành`;
}

// Escape HTML để tránh XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Event listeners
addBtn.addEventListener('click', addTask);

taskInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        addTask();
    }
});

clearCompleted.addEventListener('click', clearCompletedTasks);
