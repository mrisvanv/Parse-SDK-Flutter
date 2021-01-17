part of flutter_parse_sdk;

// ignore_for_file: always_specify_types

class ParseBatch {
  ParseBatch(List batchList, {bool debug, ParseHTTPClient client, bool autoSendSessionId}) : super() {
    _batchList = batchList;
    _path = '$keyEndPointBatch';
    _debug = isDebugEnabled(objectLevelDebug: debug);
    _client = client ??
        ParseHTTPClient(
            sendSessionId: autoSendSessionId ?? ParseCoreData().autoSendSessionId,
            securityContext: ParseCoreData().securityContext);
  }

  String _path;
  List<ParseObject> _batchList;
  bool _debug;
  ParseHTTPClient _client;

  Future<ParseResponse> processBatch() async {
    final List<ParseObject> finished = List<ParseObject>();
    final ParseResponse totalResponse = ParseResponse()
      ..success = true
      ..results = List<dynamic>()
      ..statusCode = 200;

    /* Batch requests have currently a limit of 50 packaged requests per single request
      This splitting will split the overall array into segments of upto 50 requests
      and execute them concurrently with a wrapper task for all of them. */
    final List<List<ParseObject>> chunks = <List<ParseObject>>[];
    for (int i = 0; i < _batchList.length; i += 50) {
      chunks.add(_batchList.sublist(i, min(_batchList.length, i + 50)));
    }

    for (List<ParseObject> chunk in chunks) {
      final List<dynamic> requests = chunk.map<dynamic>((ParseObject obj) {
        return obj._getRequestJson(obj.toDelete
            ? 'DELETE'
            : obj.objectId == null
                ? 'POST'
                : 'PUT');
      }).toList();
      for (ParseObject obj in chunk) {
        obj._saveChanges();
      }
      final ParseResponse response = await batchRequest(requests, chunk);
      totalResponse.success &= response.success;
      if (response.success) {
        totalResponse.results.addAll(response.results);
        totalResponse.count += response.count;
        for (int i = 0; i < response.count; i++) {
          if (response.results[i] is ParseError) {
            // Batch request succeed, but part of batch failed.
            chunk[i]._revertSavingChanges();
          } else {
            chunk[i]._savingChanges.clear();
          }
        }
      } else {
        // If there was an error, we want to roll forward the save changes before rethrowing.
        for (ParseObject obj in chunk) {
          obj._revertSavingChanges();
        }
        totalResponse.statusCode = response.statusCode;
        totalResponse.error = response.error;
      }
    }
    return totalResponse;
  }
}
