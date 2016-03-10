// onresize gets called a lot in some browsers, 
// so use this debounce function to rate limit the call to resize
var debounce = function (func) {
  var timeout;

  return function debounced () {
    var obj = this, args = arguments;
    function delayed () {
      func.apply(obj, args);
      timeout = null;
    }

    if (timeout) {
      clearTimeout(timeout);
    }

    timeout = setTimeout(delayed, 200);
  };
};

function resize() {
  var width = $('#lhs').width();
  var componentIds = ['kl','tl','tl1','tl2','minikl'];
  for(var i = 0; i < componentIds.length; i++) {
    var id = componentIds[i];
    var jq = $('#' + id);
    if(jq.length > 0) {
      if(id === 'minikl') {
        width = $('#rhs').width() - 40;
      }
      KeyLines.setSize(id, width, jq.height());
    }
  }
}

$(window).resize(debounce(resize));

$(window).load(resize);
