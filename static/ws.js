var ws = new WebSocket("ws://localhost:8000/ws");
//ws.onmessage = function (event) {
//    var message = JSON.parse(event.data);
//    console.log(message)
//}

ws.onmessage = function(event) {
    var messages = document.getElementById('messages');
    var message = document.createElement('li');
    var content = document.createTextNode(event.data);
    parsed_content = JSON.parse(event.data);
    console.log("type", parsed_content['type']);
    type = parsed_content['type'];

    if (type === 'ping') {
        ws.send(JSON.stringify({'type': 'pong'}));
    }
    else if (type === 'message') {
        //    console.log(event.data);
        message.appendChild(content);
        messages.appendChild(message);
    }
    };