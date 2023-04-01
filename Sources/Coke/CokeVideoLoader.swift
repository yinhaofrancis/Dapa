//
//  WebVideoLoader.swift
//  WebSource
//
//  Created by hao yin on 2021/5/2.
//

import Foundation

import AVFoundation

public class CokeVideoLoader:NSObject,AVAssetResourceLoaderDelegate{
    
    public static let sectionSize:UInt64 = 1024 * 1024;
    
    public var downloader:CokeSessionDownloader
    
    public init(url:URL) throws {
        self.downloader = try CokeSessionDownloader(url: url)
        super.init()
    }
    public var asset:AVAsset?{
        var c = URLComponents(string: self.downloader.url.absoluteString)
        c?.scheme = "coke"
        guard let url = c?.url else { return nil }
        let a = AVURLAsset(url: url)
        a.resourceLoader.setDelegate(self, queue: DispatchQueue(label: "CokeVideoLoader"))
        a.resourceLoader.preloadsEligibleContentKeys = true;
        return a
    }
    public var queue:DispatchQueue = {
        DispatchQueue(label: "download")
    }()
    public var noCacheAsset:AVAsset{
        return AVURLAsset(url: self.downloader.url)
    }

    public func resourceLoader(_ resourceLoader: AVAssetResourceLoader, shouldWaitForLoadingOfRequestedResource loadingRequest: AVAssetResourceLoadingRequest) -> Bool {
        self.queue.async {
            if loadingRequest.contentInformationRequest != nil{
                self.loadFileType(request: loadingRequest)
            }else{
                self.loadFileData(request: loadingRequest)
            }
        }
        return true
    }
    public func resourceLoader(_ resourceLoader: AVAssetResourceLoader, didCancel loadingRequest: AVAssetResourceLoadingRequest) {
        guard let dataReq = loadingRequest.dataRequest else { return }
        if dataReq.requestsAllDataToEndOfResource{
            self.downloader.cancel(index: UInt64(dataReq.currentOffset))
        }else{
            self.downloader.cancel(range: UInt64(dataReq.requestedOffset)...UInt64(Int(dataReq.requestedOffset) + dataReq.requestedLength - 1))
        }
    }
    fileprivate func makeSection(_ dataRequest: AVAssetResourceLoadingDataRequest) -> ClosedRange<UInt64> {
        if dataRequest.requestedLength > CokeVideoLoader.sectionSize{
            return UInt64(dataRequest.requestedOffset) ... UInt64(Int64(CokeVideoLoader.sectionSize) + Int64(dataRequest.requestedOffset - 1))
        }else{
            return UInt64(dataRequest.requestedOffset)...UInt64(dataRequest.requestedLength + Int(dataRequest.requestedOffset) - 1)
        }
    }
    
    func loadFileData(request:AVAssetResourceLoadingRequest) {
        guard let dataRequest = request.dataRequest else { return }
        if request.isFinished || request.isCancelled{
            return
        }
        if dataRequest.requestsAllDataToEndOfResource{
            if let data = self.downloader[UInt64(dataRequest.currentOffset)]{
                dataRequest.respond(with: data)
                request.finishLoading()
            }else{
                
                let n :UInt64 = UInt64(dataRequest.currentOffset) + CokeVideoLoader.sectionSize
                let end = n > self.downloader.storage.size - 1 ? UInt64(self.downloader.storage.size - 1) : n
                CokeSession.shared.beginGroup {
                    print("贪婪",UInt64(dataRequest.currentOffset) ... end)
                    try? self.downloader.download(range: UInt64(dataRequest.currentOffset) ... end)
                } notify: {
                    print("贪婪",UInt64(dataRequest.currentOffset) ... end,"成功")
                    self.loadFileData(request: request)
                }

            }
        }else{
            
            let r = makeSection(dataRequest)
            if self.downloader.storage.complete(range: r){
                if let data = self.downloader.storage[r]{
                    dataRequest.respond(with: data)
                    request.finishLoading()
                }
            }else{
                print("分段",dataRequest.currentOffset,dataRequest.requestedOffset,dataRequest.requestedLength)
                CokeSession.shared.beginGroup {
                    try? self.downloader.download(range: r)
                } notify: {
                    print("分段",dataRequest.currentOffset,dataRequest.requestedOffset,dataRequest.requestedLength,"成功")
                    self.loadFileData(request: request)
                }
            }
        }
    }
    
    func loadFileType(request:AVAssetResourceLoadingRequest) {
        if(self.downloader.storage.size > 0){
            request.contentInformationRequest?.contentLength = Int64(self.downloader.storage.size)
            if(self.downloader.storage.resourceType.contains("video")){
                let mp4 = self.downloader.storage.resourceType.contains("mp4")
                let mpeg = self.downloader.storage.resourceType.contains("mpeg")
                let mov = self.downloader.storage.resourceType.contains("quicktime")
                let m4v = self.downloader.storage.resourceType.contains("m4v")
                if mp4 || mpeg{
                    request.contentInformationRequest?.contentType = AVFileType.mp4.rawValue
                } else if mov {
                    request.contentInformationRequest?.contentType = AVFileType.mov.rawValue
                }else if m4v{
                    request.contentInformationRequest?.contentType = AVFileType.m4v.rawValue
                }else{
                    request.contentInformationRequest?.contentType = AVFileType.mp4.rawValue
                }
            }else{
                request.contentInformationRequest?.contentType = AVFileType.mp4.rawValue
            }
            request.contentInformationRequest?.isByteRangeAccessSupported = true
            request.finishLoading()
        }else{
            CokeSession.shared.beginGroup {
                self.downloader.prepare()
            } notify: {
                self.loadFileType(request: request)
            }

        }
    }
}
