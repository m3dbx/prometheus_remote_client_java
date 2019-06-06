// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the 'Software'), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package com.uber.m3.promremoteclient;

import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.RequestBody;
import okhttp3.Request;
import okhttp3.MediaType;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class Client {

    private OkHttpClient client;

    private String writeUrl;

    public Client(String writeUrl) {
        this.client = new OkHttpClient();
        this.writeUrl = writeUrl;
    }

    public void WriteProto(Prometheus.WriteRequest req) throws IOException {
        byte[] compressed = Snappy.compress(req.toByteArray());
        MediaType mediaType = MediaType.parse("application/x-protobuf");
        RequestBody body = RequestBody.create(mediaType, compressed);
        Request request = new Request.Builder()
            .url(writeUrl)
            .addHeader("Content-Encoding", "snappy")
            .addHeader("User-Agent", "promremote-java/0.1.0")
            .addHeader("X-Prometheus-Remote-Write-Version", "0.1.0")
            .post(body)
            .build();
        Response response = client.newCall(request).execute();
        if (response.code() != 200) {
            throw new IOException(
                "expected 200 status code: actual=" + String.valueOf(response.code()) + ", " +
                "body=" + response.message());
        }
    }

}
