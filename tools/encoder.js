
function hashCode(s) {
  return s.split("").reduce(function(a, b) {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
}

function encodeURL(url, salt) {
    let hash = hashCode(url + salt );
    let encoded = encodeURIComponent(btoa(url + '|' + hash));
    return encoded;
}


$(document).ready(function() {
    $("button").click(function() {
        let url = $("#url").val();
        let salt = $("#salt").val();
        $("#result").val(encodeURL(url, salt));
    });
});