(function dsbuilder(attr) {
    var urlBuilder = "jdbc:avatica:remote:url=http://" + attr[connectionHelper.attributeServer] + ":" + attr[connectionHelper.attributePort] + ";serialization=PROTOBUF;database=" + attr[connectionHelper.attributeDatabase];

    return [urlBuilder];
})
