# MongoDB Query Examples

## number of valid results
'''
db.validity.aggregate(
    [
        {
            $sort: {
                prefix: 1, timestamp: 1
            }
        },
        {
            $group: {
                _id: "$prefix",
                timestamp: { $max: "$timestamp" },
                validated_route: { $addToSet: "$validated_route"},
                type: { $addToSet: "$type" }
            }
        },
        {
            $match: {
                'validated_route.validity.state' : 'Valid'
            }
        },
        {
            $match: {
                'type' : 'announcement'
            }
        },
        {
            $group: {
                _id: null,
                count: {$sum: 1}
            }
        }
    ]
).result[0].count
'''

'''
db.archive.aggregate(
    [
        {
            $sort: {
                prefix: 1, timestamp: 1
            }
        },
        {
            $group: {
                _id: "$prefix",
                timestamp: { $max: "$timestamp" },
                validated_route: { $addToSet: "$validated_route"},
                type: { $addToSet: "$type" }
            }
        },
        {
            $match: {
                'type' : 'announcement'
            }
        },
        {
            $group: {
                _id: null,
                count: {$sum: 1}
            }
        }
    ]
).result[0].count
'''


db.validity.aggregate(
    [
        {
            $match: {
                'type' : 'announcement'
            }
        },
        {
            $sort: {
                prefix: 1, timestamp: 1
            }
        },
        {
            $group: {
                _id: "$prefix",
                timestamp: { $max: "$timestamp" },
                validity: { $first: "$validated_route.validity.state"}
            }
        },
        {
            $group: {
                _id: "$validity",
                count: {$sum: 1}
            }
        }
    ]
).result[0].count
