## map function
```
var f_map = function() {
    var key = this.prefix;
    var value = {timestamp: this.timestamp, type: this.type, validated_route: null};
    if (this.type == "announcement") {
        value['validated_route'] = this.validated_route;
    }
    emit (key, value);
};
```

## reduce function
```
var f_reduce = function (key, values) {
    var robj = {timestamp: 0, type: null, validated_route: null};
    values.forEach(
        function(value) {
            if ((value != null) && (value.timestamp > robj.timestamp)) {
                robj.prefix = key;
                robj.timestamp = value.timestamp;
                robj.type = value.type;
                if (value.type == "announcement") {
                    robj.validated_route = value.validated_route;
                }
                else {
                    robj.validated_route = null;
                }
            }
        }
    );
    return robj;
};
```

## all-in-one

```
db.validity.mapReduce(f_map, f_reduce, {out: {replace: "validity_latest"} });
```
