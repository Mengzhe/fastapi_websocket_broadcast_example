var ws = new WebSocket("ws://localhost:8000/ws");
//ws.onmessage = function (event) {
//    var message = JSON.parse(event.data);
//    console.log(message)
//}

ws.onmessage = function(event) {
    var messages = document.getElementById('messages');
    var message = document.createElement('li');
    var content = document.createTextNode(event.data);
//    console.log(content);
    message.appendChild(content);
    messages.appendChild(message);
    };