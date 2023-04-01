//
//  File.swift
//  
//
//  Created by wenyang on 2023/2/16.
//

import AVFoundation

public protocol CokePlayerDelegate:NSObject{
    func updateTime(player:CokePlayer,percent:Double,time:CMTime)
    func stateChange(player:CokePlayer,status:AVPlayer.TimeControlStatus)
    func videoSize(player:CokePlayer,size:CGSize)
    func videoStart(player:CokePlayer)
}

public enum CokePlayerPlayMode{
    case endPause
    case endReverse
}

public class CokePlayer{
    
    public var player:AVPlayer

    private var loader:CokeVideoLoader
    
    public weak var delegate:CokePlayerDelegate?
    
    public var generator:AVAssetImageGenerator
    
    public var state:AVPlayer.TimeControlStatus{
        return self.player.timeControlStatus
    }
    public private(set) var isSeek:Bool = false
    
    public var playMode:CokePlayerPlayMode = .endReverse
    
    public var playRate:Float = 1{
        didSet{
            self.player.rate = self.playRate
        }
    }
    public var duration:CMTime{
        self.player.currentItem?.asset.duration ?? .zero
    }
    
    private var lastState:AVPlayer.TimeControlStatus = .paused
    private var boundObs:Any?
    
    public init(url:URL,videoFile:Bool = true,delegate:CokePlayerDelegate) throws{
        if videoFile{
            self.loader = try CokeVideoLoader(url: url)
            guard let asset = self.loader.asset else { throw NSError(domain: "CokePlayer load Fail", code: 0)}
            self.player = AVPlayer(playerItem: AVPlayerItem(asset: asset))
            self.generator = AVAssetImageGenerator(asset: asset)
        }else{
            self.loader = try CokeVideoLoader(url: url)
            let asset = self.loader.noCacheAsset
            self.player = AVPlayer(playerItem: AVPlayerItem(asset: asset))
            self.generator = AVAssetImageGenerator(asset: asset)
        }
        self.delegate = delegate
        self.assetAsyncLoad()
    }
    private func loadBound(){
        if (boundObs == nil){
            self.boundObs = self.player.addBoundaryTimeObserver(forTimes: [NSValue(time: duration)], queue: DispatchQueue.main) { [weak self] in
                guard let ws = self else { return }
                switch(ws.playMode){
                case .endPause:
                    ws.pause()
                    break;
                case .endReverse:
                    ws.seek(percent: 0)
                    DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(1), execute: {
                        ws.play()
                    })
                    break
                }
            }
        }
    }
    public func image(percent:Float,callback:@escaping (CGImage?)->Void){
        if let time = self.timePercent(percent: percent){
            self.generator.generateCGImagesAsynchronously(forTimes: [NSValue(time: time)]) { _, img, _, r, e in
                callback(img)
            }
        }
    }
    fileprivate func assetAsyncLoad() {
        let time =  CMTime(seconds: 1,
                           preferredTimescale: CMTimeScale(NSEC_PER_SEC))
        self.player.addPeriodicTimeObserver(forInterval: time, queue: DispatchQueue.main) { [weak self] time in
            guard let ws = self else { return }
            if(ws.lastState != ws.player.timeControlStatus){
                ws.delegate?.stateChange(player: ws, status: ws.player.timeControlStatus)
                ws.lastState = ws.player.timeControlStatus
            }
            guard ws.duration != .zero else { return }
            ws.delegate?.updateTime(player: ws, percent: time.seconds / ws.duration.seconds, time: time)
        }
        
        if let ass = self.player.currentItem?.asset{
            ass.load(values: "tracks","playable","naturalSize","duration") { [weak self] i in
                guard let self else { return }
                if(i == "playable"){
                    DispatchQueue.main.async {
                        self.delegate?.videoStart(player: self)
                    }
                }
                if(i == "duration"){
                    self.loadBound()
                }
                if(i == "naturalSize"){
                    guard let size = ass.tracks(withMediaType: .video).first?.naturalSize else { return }
                    DispatchQueue.main.async {
                        self.delegate?.videoSize(player: self, size: size)
                    }
                }
            }
        }
    }
    
    public func play() {
        self.isSeek = false
        if let ass = self.player.currentItem?.asset{
            ass.load(values: "playable") { _ in
                print("开始播放")
                self.player.play()
                self.playRate = self.playRate
            }
        }
    }
    public func pause(){
        self.player.pause()
    }
    
    public func seek(percent:Float){
        if let time = self.timePercent(percent: percent){
            if time.isValid{
                self.isSeek = true
                self.player.pause()
                self.player.seek(to: time, toleranceBefore: CMTime(value: 0, timescale: time.timescale), toleranceAfter: CMTime(value: 0, timescale: time.timescale))
            }
        }
    }
    public func timePercent(percent:Float)->CMTime?{
        if(percent <= 0.01){
            return .zero
        }
        if let dur = self.player.currentItem?.duration{
            return CMTime(value: Int64(Float(dur.value) * percent), timescale: dur.timescale)
        }
        return nil
    }
    public static func preload(url:URL,size:UInt64 = 0){
       guard let loader = try? CokeVideoLoader(url: url) else { return }
       if nil == loader.downloader[0]{
           CokeSession.shared.beginGroup {
               loader.downloader.prepare()
           } notify: {
               let c = size == 0 ? loader.downloader.storage.size / 10 : size
               CokeSession.shared.beginGroup {
                   try? loader.downloader.download(range: 0 ... c)
               } notify: {
                   
               }
           }
       }
   }
}

@objc public protocol CokeVideoViewDelegate:NSObjectProtocol{
    @objc optional func updateTime(view:CokeVideoView,percent:Double,current:CMTime,duration:CMTime)
    @objc optional func stateChange(view:CokeVideoView,status:AVPlayer.TimeControlStatus)
    @objc optional func imageCover(view:CokeVideoView,image:CGImage)
    @objc optional func videoStart(view:CokeVideoView)
}

@objc(CKVideoView)
public class CokeVideoView:UIView,CokePlayerDelegate {
    public func videoStart(player: CokePlayer) {
        self.delegate?.videoStart?(view: self)
    }
    public func videoSize(player: CokePlayer, size: CGSize) {
        let ratio = size.width / size.height
        if((self.ratio) != nil){
            self.removeConstraint(self.ratio!)
        }
        self.ratio = self.videoLayoutGuide.widthAnchor.constraint(equalTo: self.videoLayoutGuide.heightAnchor, multiplier: ratio)
        self.ratio?.priority = .defaultHigh
        self.addConstraint(self.ratio!)
    }
    
    public func updateTime(player: CokePlayer, percent: Double,time:CMTime) {
        self.delegate?.updateTime?(view: self, percent: percent, current: time, duration: player.duration)
    }
    
    public func stateChange(player: CokePlayer, status: AVPlayer.TimeControlStatus) {
        self.delegate?.stateChange?(view: self, status: status)
    }
    private var image:CGImage?
    
    public var player:CokePlayer?{
        didSet{
            self.playerLayer.player = self.player?.player
        }
    }
    public var playerLayer:AVPlayerLayer{
        return self.layer as! AVPlayerLayer
    }
    public override class var layerClass: AnyClass{
        return AVPlayerLayer.self
    }
    
    private var ratio:NSLayoutConstraint?
    private var portrait:[NSLayoutConstraint] = []
    private var landscape:[NSLayoutConstraint] = []
    @objc public var videoLayoutGuide:UILayoutGuide

    public override init(frame: CGRect) {
        let g = UILayoutGuide()
        self.videoLayoutGuide = g
        super.init(frame: frame)
        self.loadGuide(g: g)
    }
    public required init?(coder: NSCoder) {
        let g = UILayoutGuide()
        self.videoLayoutGuide = g
        super.init(coder: coder)
        self.loadGuide(g: g)
    }
    private func loadGuide(g:UILayoutGuide){
        self.addLayoutGuide(g)
        self.portrait = [
            g.leadingAnchor.constraint(equalTo: self.leadingAnchor),
            g.trailingAnchor.constraint(equalTo: self.trailingAnchor),
            g.centerYAnchor.constraint(equalTo: self.centerYAnchor)
        ]
        self.landscape = [
            g.topAnchor.constraint(equalTo: self.topAnchor),
            g.bottomAnchor.constraint(equalTo: self.bottomAnchor),
            g.centerXAnchor.constraint(equalTo: self.centerXAnchor)
        ]
        self.ratio = self.videoLayoutGuide.widthAnchor.constraint(equalTo: self.videoLayoutGuide.heightAnchor, multiplier: 16.0 / 9.0)
        self.ratio?.priority = .defaultHigh
        self.addConstraint(self.ratio!)
        self.addConstraints(self.portrait)
        self.addConstraints(self.landscape)
        self.landscape.forEach { i in
            i.isActive = false;
        }
    }
    
    @objc public func loadVideo(url:URL,noCache:Bool) throws{
        self.player = try CokePlayer(url: url,videoFile:!noCache, delegate: self)
        self.player?.delegate = self
        self.loadCover()
    }
    private func loadCover(){
        self.player?.image(percent: 0, callback: { [weak self] img in
            guard let self else { return }
            guard let img else {
                self.loadCover()
                return
            }
            DispatchQueue.main.async {
                self.delegate?.imageCover?(view: self, image: img)
            }
        })
    }
    @objc public static func clean(url:URL){
        try? CokeVideoLoader(url: url).downloader.storage.delete()
    }
    @objc public static func preload(url:URL,size:UInt64 = 100 * 1024){
        CokePlayer.preload(url: url, size: size)
    }
    @objc public weak var delegate:CokeVideoViewDelegate?
    
    @objc public var isSeek:Bool{
        return self.player?.isSeek ?? false
    }
    @objc public func seek(percent:Float){
        self.player?.seek(percent: percent)
    }
    @objc public func play(){
        self.player?.play()
    }
    @objc public func pause(){
        self.player?.pause()
    }
    @objc public var state:AVPlayer.TimeControlStatus{
        self.player?.state ?? .paused
    }
    @objc public func rate(rate:Float){
        self.player?.playRate = rate
    }
    
    public override func traitCollectionDidChange(_ previousTraitCollection: UITraitCollection?) {
        if self.traitCollection.verticalSizeClass == .regular{
            self.portrait.forEach { i in
                i.isActive = true
            }
            self.landscape.forEach { i in
                i.isActive = false
            }
        }else{
            self.portrait.forEach { i in
                i.isActive = false
            }
            self.landscape.forEach { i in
                i.isActive = true
            }
        }
    }
}


extension AVAsset{
    public func load(values:String...,callback:@escaping (String)->Void){
        self.loadValuesAsynchronously(forKeys: values) {
            values.forEach { s in
                var error:NSError?
                if self.statusOfValue(forKey:s, error: &error) == .loaded{
                    callback(s)
                }
            }
        }
    }
}
