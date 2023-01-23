
function hashCode(s) {
  return s.split("").reduce(function(a, b) {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
}

function encodeURL(url, salt) {
    let hash = hashCode(url + salt );
    let encoded = encodeURIComponent(btoa(url + '|' + hash));
    return {
        'encoded': encoded,
        'hash': hash
    };
}


$(document).ready(function() {
    $("button").click(function() {
        let url = $("#url").val();
        let salt = $("#salt").val();
        let encoded = encodeURL(url, salt);
        $("#result").val(encoded.encoded);
        $("#hash").val(encoded.hash);
    });
});