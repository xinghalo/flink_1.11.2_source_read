/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ENTER } from '@angular/cdk/keycodes';
import { HttpClient, HttpEventType, HttpHeaders, HttpRequest, HttpResponse } from '@angular/common/http';
import { Component, ElementRef, HostListener, Input, Optional, ViewChild, ViewEncapsulation } from '@angular/core';
import { of, Observable, Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
var NzUploadBtnComponent = /** @class */ (function () {
    // #endregion
    function NzUploadBtnComponent(http, el, updateHostClassService) {
        this.http = http;
        this.el = el;
        this.updateHostClassService = updateHostClassService;
        this.reqs = {};
        this.inited = false;
        this.destroy = false;
        // #region fields
        this.classes = {};
        // #region styles
        this.prefixCls = 'ant-upload';
        if (!http) {
            throw new Error("Not found 'HttpClient', You can import 'HttpClientModule' in your root module.");
        }
    }
    // #endregion
    // #endregion
    /**
     * @return {?}
     */
    NzUploadBtnComponent.prototype.onClick = 
    // #endregion
    /**
     * @return {?}
     */
    function () {
        if (this.options.disabled || !this.options.openFileDialogOnClick) {
            return;
        }
        ((/** @type {?} */ (this.file.nativeElement))).click();
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzUploadBtnComponent.prototype.onKeyDown = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (this.options.disabled) {
            return;
        }
        if (e.key === 'Enter' || e.keyCode === ENTER) {
            this.onClick();
        }
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzUploadBtnComponent.prototype.onFileDrop = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        var _this = this;
        if (this.options.disabled || e.type === 'dragover') {
            e.preventDefault();
            return;
        }
        if (this.options.directory) {
            this.traverseFileTree((/** @type {?} */ (e.dataTransfer)).items);
        }
        else {
            /** @type {?} */
            var files = Array.prototype.slice
                .call((/** @type {?} */ (e.dataTransfer)).files)
                .filter((/**
             * @param {?} file
             * @return {?}
             */
            function (file) { return _this.attrAccept(file, _this.options.accept); }));
            if (files.length) {
                this.uploadFiles(files);
            }
        }
        e.preventDefault();
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzUploadBtnComponent.prototype.onChange = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (this.options.disabled) {
            return;
        }
        /** @type {?} */
        var hie = (/** @type {?} */ (e.target));
        this.uploadFiles((/** @type {?} */ (hie.files)));
        hie.value = '';
    };
    /**
     * @private
     * @param {?} files
     * @return {?}
     */
    NzUploadBtnComponent.prototype.traverseFileTree = /**
     * @private
     * @param {?} files
     * @return {?}
     */
    function (files) {
        var _this = this;
        var e_1, _a;
        // tslint:disable-next-line:no-any
        /** @type {?} */
        var _traverseFileTree = (/**
         * @param {?} item
         * @param {?} path
         * @return {?}
         */
        function (item, path) {
            if (item.isFile) {
                item.file((/**
                 * @param {?} file
                 * @return {?}
                 */
                function (file) {
                    if (_this.attrAccept(file, _this.options.accept)) {
                        _this.uploadFiles([file]);
                    }
                }));
            }
            else if (item.isDirectory) {
                /** @type {?} */
                var dirReader = item.createReader();
                // tslint:disable-next-line:no-any
                dirReader.readEntries((/**
                 * @param {?} entries
                 * @return {?}
                 */
                function (entries) {
                    var e_2, _a;
                    try {
                        for (var entries_1 = tslib_1.__values(entries), entries_1_1 = entries_1.next(); !entries_1_1.done; entries_1_1 = entries_1.next()) {
                            var entrieItem = entries_1_1.value;
                            _traverseFileTree(entrieItem, "" + path + item.name + "/");
                        }
                    }
                    catch (e_2_1) { e_2 = { error: e_2_1 }; }
                    finally {
                        try {
                            if (entries_1_1 && !entries_1_1.done && (_a = entries_1.return)) _a.call(entries_1);
                        }
                        finally { if (e_2) throw e_2.error; }
                    }
                }));
            }
        });
        try {
            // tslint:disable-next-line:no-any
            for (var _b = tslib_1.__values((/** @type {?} */ (files))), _c = _b.next(); !_c.done; _c = _b.next()) {
                var file = _c.value;
                _traverseFileTree(file.webkitGetAsEntry(), '');
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
    };
    /**
     * @private
     * @param {?} file
     * @param {?=} acceptedFiles
     * @return {?}
     */
    NzUploadBtnComponent.prototype.attrAccept = /**
     * @private
     * @param {?} file
     * @param {?=} acceptedFiles
     * @return {?}
     */
    function (file, acceptedFiles) {
        if (file && acceptedFiles) {
            /** @type {?} */
            var acceptedFilesArray = Array.isArray(acceptedFiles) ? acceptedFiles : acceptedFiles.split(',');
            /** @type {?} */
            var fileName_1 = '' + file.name;
            /** @type {?} */
            var mimeType_1 = '' + file.type;
            /** @type {?} */
            var baseMimeType_1 = mimeType_1.replace(/\/.*$/, '');
            return acceptedFilesArray.some((/**
             * @param {?} type
             * @return {?}
             */
            function (type) {
                /** @type {?} */
                var validType = type.trim();
                if (validType.charAt(0) === '.') {
                    return (fileName_1
                        .toLowerCase()
                        .indexOf(validType.toLowerCase(), fileName_1.toLowerCase().length - validType.toLowerCase().length) !== -1);
                }
                else if (/\/\*$/.test(validType)) {
                    // This is something like a image/* mime type
                    return baseMimeType_1 === validType.replace(/\/.*$/, '');
                }
                return mimeType_1 === validType;
            }));
        }
        return true;
    };
    /**
     * @private
     * @param {?} file
     * @return {?}
     */
    NzUploadBtnComponent.prototype.attachUid = /**
     * @private
     * @param {?} file
     * @return {?}
     */
    function (file) {
        if (!file.uid) {
            file.uid = Math.random()
                .toString(36)
                .substring(2);
        }
        return file;
    };
    /**
     * @param {?} fileList
     * @return {?}
     */
    NzUploadBtnComponent.prototype.uploadFiles = /**
     * @param {?} fileList
     * @return {?}
     */
    function (fileList) {
        var _this = this;
        /** @type {?} */
        var filters$ = of(Array.prototype.slice.call(fileList));
        if (this.options.filters) {
            this.options.filters.forEach((/**
             * @param {?} f
             * @return {?}
             */
            function (f) {
                filters$ = filters$.pipe(switchMap((/**
                 * @param {?} list
                 * @return {?}
                 */
                function (list) {
                    /** @type {?} */
                    var fnRes = f.fn(list);
                    return fnRes instanceof Observable ? fnRes : of(fnRes);
                })));
            }));
        }
        filters$.subscribe((/**
         * @param {?} list
         * @return {?}
         */
        function (list) {
            list.forEach((/**
             * @param {?} file
             * @return {?}
             */
            function (file) {
                _this.attachUid(file);
                _this.upload(file, list);
            }));
        }), (/**
         * @param {?} e
         * @return {?}
         */
        function (e) {
            console.warn("Unhandled upload filter error", e);
        }));
    };
    /**
     * @private
     * @param {?} file
     * @param {?} fileList
     * @return {?}
     */
    NzUploadBtnComponent.prototype.upload = /**
     * @private
     * @param {?} file
     * @param {?} fileList
     * @return {?}
     */
    function (file, fileList) {
        var _this = this;
        if (!this.options.beforeUpload) {
            return this.post(file);
        }
        /** @type {?} */
        var before = this.options.beforeUpload(file, fileList);
        if (before instanceof Observable) {
            before.subscribe((/**
             * @param {?} processedFile
             * @return {?}
             */
            function (processedFile) {
                /** @type {?} */
                var processedFileType = Object.prototype.toString.call(processedFile);
                if (processedFileType === '[object File]' || processedFileType === '[object Blob]') {
                    _this.attachUid(processedFile);
                    _this.post(processedFile);
                }
                else if (typeof processedFile === 'boolean' && processedFile !== false) {
                    _this.post(file);
                }
            }), (/**
             * @param {?} e
             * @return {?}
             */
            function (e) {
                console.warn("Unhandled upload beforeUpload error", e);
            }));
        }
        else if (before !== false) {
            return this.post(file);
        }
    };
    /**
     * @private
     * @param {?} file
     * @return {?}
     */
    NzUploadBtnComponent.prototype.post = /**
     * @private
     * @param {?} file
     * @return {?}
     */
    function (file) {
        var _this = this;
        if (this.destroy) {
            return;
        }
        /** @type {?} */
        var opt = this.options;
        var uid = file.uid;
        var data = opt.data, headers = opt.headers;
        if (typeof data === 'function') {
            data = ((/** @type {?} */ (data)))(file);
        }
        if (typeof headers === 'function') {
            headers = ((/** @type {?} */ (headers)))(file);
        }
        /** @type {?} */
        var args = {
            action: opt.action,
            name: opt.name,
            headers: headers,
            file: file,
            data: data,
            withCredentials: opt.withCredentials,
            onProgress: opt.onProgress
                ? (/**
                 * @param {?} e
                 * @return {?}
                 */
                function (e) {
                    (/** @type {?} */ (opt.onProgress))(e, file);
                })
                : undefined,
            onSuccess: (/**
             * @param {?} ret
             * @param {?} xhr
             * @return {?}
             */
            function (ret, xhr) {
                _this.clean(uid);
                (/** @type {?} */ (opt.onSuccess))(ret, file, xhr);
            }),
            onError: (/**
             * @param {?} xhr
             * @return {?}
             */
            function (xhr) {
                _this.clean(uid);
                (/** @type {?} */ (opt.onError))(xhr, file);
            })
        };
        /** @type {?} */
        var req$ = (opt.customRequest || this.xhr).call(this, args);
        if (!(req$ instanceof Subscription)) {
            console.warn("Must return Subscription type in '[nzCustomRequest]' property");
        }
        this.reqs[uid] = req$;
        (/** @type {?} */ (opt.onStart))(file);
    };
    /**
     * @private
     * @param {?} args
     * @return {?}
     */
    NzUploadBtnComponent.prototype.xhr = /**
     * @private
     * @param {?} args
     * @return {?}
     */
    function (args) {
        var _this = this;
        /** @type {?} */
        var formData = new FormData();
        // tslint:disable-next-line:no-any
        formData.append((/** @type {?} */ (args.name)), (/** @type {?} */ (args.file)));
        if (args.data) {
            Object.keys(args.data).map((/**
             * @param {?} key
             * @return {?}
             */
            function (key) {
                formData.append(key, (/** @type {?} */ (args.data))[key]);
            }));
        }
        if (!args.headers) {
            args.headers = {};
        }
        if (args.headers['X-Requested-With'] !== null) {
            args.headers['X-Requested-With'] = "XMLHttpRequest";
        }
        else {
            delete args.headers['X-Requested-With'];
        }
        /** @type {?} */
        var req = new HttpRequest('POST', (/** @type {?} */ (args.action)), formData, {
            reportProgress: true,
            withCredentials: args.withCredentials,
            headers: new HttpHeaders(args.headers)
        });
        return this.http.request(req).subscribe((/**
         * @param {?} event
         * @return {?}
         */
        function (event) {
            if (event.type === HttpEventType.UploadProgress) {
                if ((/** @type {?} */ (event.total)) > 0) {
                    // tslint:disable-next-line:no-any
                    ((/** @type {?} */ (event))).percent = (event.loaded / (/** @type {?} */ (event.total))) * 100;
                }
                (/** @type {?} */ (args.onProgress))(event, args.file);
            }
            else if (event instanceof HttpResponse) {
                (/** @type {?} */ (args.onSuccess))(event.body, args.file, event);
            }
        }), (/**
         * @param {?} err
         * @return {?}
         */
        function (err) {
            _this.abort(args.file);
            (/** @type {?} */ (args.onError))(err, args.file);
        }));
    };
    /**
     * @private
     * @param {?} uid
     * @return {?}
     */
    NzUploadBtnComponent.prototype.clean = /**
     * @private
     * @param {?} uid
     * @return {?}
     */
    function (uid) {
        /** @type {?} */
        var req$ = this.reqs[uid];
        if (req$ instanceof Subscription) {
            req$.unsubscribe();
        }
        delete this.reqs[uid];
    };
    /**
     * @param {?=} file
     * @return {?}
     */
    NzUploadBtnComponent.prototype.abort = /**
     * @param {?=} file
     * @return {?}
     */
    function (file) {
        var _this = this;
        if (file) {
            this.clean(file && file.uid);
        }
        else {
            Object.keys(this.reqs).forEach((/**
             * @param {?} uid
             * @return {?}
             */
            function (uid) { return _this.clean(uid); }));
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzUploadBtnComponent.prototype.setClassMap = /**
     * @private
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var classMap = tslib_1.__assign((_a = {}, _a[this.prefixCls] = true, _a[this.prefixCls + "-disabled"] = this.options.disabled, _a), this.classes);
        this.updateHostClassService.updateHostClass(this.el.nativeElement, classMap);
    };
    /**
     * @return {?}
     */
    NzUploadBtnComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.inited = true;
        this.setClassMap();
    };
    /**
     * @return {?}
     */
    NzUploadBtnComponent.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        if (this.inited) {
            this.setClassMap();
        }
    };
    /**
     * @return {?}
     */
    NzUploadBtnComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy = true;
        this.abort();
    };
    NzUploadBtnComponent.decorators = [
        { type: Component, args: [{
                    selector: '[nz-upload-btn]',
                    template: "<input type=\"file\" #file (change)=\"onChange($event)\"\n  [attr.accept]=\"options.accept\"\n  [attr.directory]=\"options.directory ? 'directory': null\"\n  [attr.webkitdirectory]=\"options.directory ? 'webkitdirectory': null\"\n  [multiple]=\"options.multiple\" style=\"display: none;\">\n<ng-content></ng-content>",
                    host: {
                        '[attr.tabindex]': '"0"',
                        '[attr.role]': '"button"'
                    },
                    providers: [NzUpdateHostClassService],
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None
                }] }
    ];
    /** @nocollapse */
    NzUploadBtnComponent.ctorParameters = function () { return [
        { type: HttpClient, decorators: [{ type: Optional }] },
        { type: ElementRef },
        { type: NzUpdateHostClassService }
    ]; };
    NzUploadBtnComponent.propDecorators = {
        file: [{ type: ViewChild, args: ['file',] }],
        classes: [{ type: Input }],
        options: [{ type: Input }],
        onClick: [{ type: HostListener, args: ['click',] }],
        onKeyDown: [{ type: HostListener, args: ['keydown', ['$event'],] }],
        onFileDrop: [{ type: HostListener, args: ['drop', ['$event'],] }, { type: HostListener, args: ['dragover', ['$event'],] }]
    };
    return NzUploadBtnComponent;
}());
export { NzUploadBtnComponent };
if (false) {
    /** @type {?} */
    NzUploadBtnComponent.prototype.reqs;
    /**
     * @type {?}
     * @private
     */
    NzUploadBtnComponent.prototype.inited;
    /**
     * @type {?}
     * @private
     */
    NzUploadBtnComponent.prototype.destroy;
    /** @type {?} */
    NzUploadBtnComponent.prototype.file;
    /** @type {?} */
    NzUploadBtnComponent.prototype.classes;
    /** @type {?} */
    NzUploadBtnComponent.prototype.options;
    /**
     * @type {?}
     * @private
     */
    NzUploadBtnComponent.prototype.prefixCls;
    /**
     * @type {?}
     * @private
     */
    NzUploadBtnComponent.prototype.http;
    /**
     * @type {?}
     * @private
     */
    NzUploadBtnComponent.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzUploadBtnComponent.prototype.updateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdXBsb2FkLWJ0bi5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidXBsb2FkL256LXVwbG9hZC1idG4uY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUFFLEtBQUssRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBQzlDLE9BQU8sRUFBRSxVQUFVLEVBQWEsYUFBYSxFQUFFLFdBQVcsRUFBRSxXQUFXLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEgsT0FBTyxFQUNMLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLEtBQUssRUFJTCxRQUFRLEVBQ1IsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsRUFBRSxFQUFFLFVBQVUsRUFBRSxZQUFZLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFDcEQsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRTNDLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBSXRGO0lBc1NFLGFBQWE7SUFFYiw4QkFDc0IsSUFBZ0IsRUFDNUIsRUFBYyxFQUNkLHNCQUFnRDtRQUZwQyxTQUFJLEdBQUosSUFBSSxDQUFZO1FBQzVCLE9BQUUsR0FBRixFQUFFLENBQVk7UUFDZCwyQkFBc0IsR0FBdEIsc0JBQXNCLENBQTBCO1FBL1IxRCxTQUFJLEdBQW9DLEVBQUUsQ0FBQztRQUNuQyxXQUFNLEdBQUcsS0FBSyxDQUFDO1FBQ2YsWUFBTyxHQUFHLEtBQUssQ0FBQzs7UUFLZixZQUFPLEdBQU8sRUFBRSxDQUFDOztRQXdRbEIsY0FBUyxHQUFHLFlBQVksQ0FBQztRQWtCL0IsSUFBSSxDQUFDLElBQUksRUFBRTtZQUNULE1BQU0sSUFBSSxLQUFLLENBQUMsZ0ZBQWdGLENBQUMsQ0FBQztTQUNuRztJQUNILENBQUM7SUEx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QyxNQUFNOzBCQUdoQixLQUFLOzBCQUNMLEtBQUs7MEJBSUwsWUFBWSxTQUFDLE9BQU87NEJBUXBCLFlBQVksU0FBQyxTQUFTLEVBQUUsQ0FBQyxRQUFRLENBQUM7NkJBVWxDLFlBQVksU0FBQyxNQUFNLEVBQUUsQ0FBQyxRQUFRLENBQUMsY0FDL0IsWUFBWSxTQUFDLFVBQVUsRUFBRSxDQUFDLFFBQVEsQ0FBQzs7SUFzUnRDLDJCQUFDO0NBQUEsQUFqVUQsSUFpVUM7U0F0VFksb0JBQW9COzs7SUFDL0Isb0NBQTJDOzs7OztJQUMzQyxzQ0FBdUI7Ozs7O0lBQ3ZCLHVDQUF3Qjs7SUFFeEIsb0NBQW9DOztJQUdwQyx1Q0FBMEI7O0lBQzFCLHVDQUFtQzs7Ozs7SUF1UW5DLHlDQUFpQzs7Ozs7SUFjL0Isb0NBQW9DOzs7OztJQUNwQyxrQ0FBc0I7Ozs7O0lBQ3RCLHNEQUF3RCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEVOVEVSIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL2tleWNvZGVzJztcbmltcG9ydCB7IEh0dHBDbGllbnQsIEh0dHBFdmVudCwgSHR0cEV2ZW50VHlwZSwgSHR0cEhlYWRlcnMsIEh0dHBSZXF1ZXN0LCBIdHRwUmVzcG9uc2UgfSBmcm9tICdAYW5ndWxhci9jb21tb24vaHR0cCc7XG5pbXBvcnQge1xuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEhvc3RMaXN0ZW5lcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE9wdGlvbmFsLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgb2YsIE9ic2VydmFibGUsIFN1YnNjcmlwdGlvbiB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgc3dpdGNoTWFwIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuXG5pbXBvcnQgeyBVcGxvYWRGaWxlLCBVcGxvYWRYSFJBcmdzLCBaaXBCdXR0b25PcHRpb25zIH0gZnJvbSAnLi9pbnRlcmZhY2UnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICdbbnotdXBsb2FkLWJ0bl0nLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotdXBsb2FkLWJ0bi5jb21wb25lbnQuaHRtbCcsXG4gIGhvc3Q6IHtcbiAgICAnW2F0dHIudGFiaW5kZXhdJzogJ1wiMFwiJyxcbiAgICAnW2F0dHIucm9sZV0nOiAnXCJidXR0b25cIidcbiAgfSxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmVcbn0pXG5leHBvcnQgY2xhc3MgTnpVcGxvYWRCdG5Db21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcywgT25EZXN0cm95IHtcbiAgcmVxczogeyBba2V5OiBzdHJpbmddOiBTdWJzY3JpcHRpb24gfSA9IHt9O1xuICBwcml2YXRlIGluaXRlZCA9IGZhbHNlO1xuICBwcml2YXRlIGRlc3Ryb3kgPSBmYWxzZTtcblxuICBAVmlld0NoaWxkKCdmaWxlJykgZmlsZTogRWxlbWVudFJlZjtcblxuICAvLyAjcmVnaW9uIGZpZWxkc1xuICBASW5wdXQoKSBjbGFzc2VzOiB7fSA9IHt9O1xuICBASW5wdXQoKSBvcHRpb25zOiBaaXBCdXR0b25PcHRpb25zO1xuXG4gIC8vICNlbmRyZWdpb25cblxuICBASG9zdExpc3RlbmVyKCdjbGljaycpXG4gIG9uQ2xpY2soKTogdm9pZCB7XG4gICAgaWYgKHRoaXMub3B0aW9ucy5kaXNhYmxlZCB8fCAhdGhpcy5vcHRpb25zLm9wZW5GaWxlRGlhbG9nT25DbGljaykge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICAodGhpcy5maWxlLm5hdGl2ZUVsZW1lbnQgYXMgSFRNTElucHV0RWxlbWVudCkuY2xpY2soKTtcbiAgfVxuXG4gIEBIb3N0TGlzdGVuZXIoJ2tleWRvd24nLCBbJyRldmVudCddKVxuICBvbktleURvd24oZTogS2V5Ym9hcmRFdmVudCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm9wdGlvbnMuZGlzYWJsZWQpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgaWYgKGUua2V5ID09PSAnRW50ZXInIHx8IGUua2V5Q29kZSA9PT0gRU5URVIpIHtcbiAgICAgIHRoaXMub25DbGljaygpO1xuICAgIH1cbiAgfVxuXG4gIEBIb3N0TGlzdGVuZXIoJ2Ryb3AnLCBbJyRldmVudCddKVxuICBASG9zdExpc3RlbmVyKCdkcmFnb3ZlcicsIFsnJGV2ZW50J10pXG4gIG9uRmlsZURyb3AoZTogRHJhZ0V2ZW50KTogdm9pZCB7XG4gICAgaWYgKHRoaXMub3B0aW9ucy5kaXNhYmxlZCB8fCBlLnR5cGUgPT09ICdkcmFnb3ZlcicpIHtcbiAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgaWYgKHRoaXMub3B0aW9ucy5kaXJlY3RvcnkpIHtcbiAgICAgIHRoaXMudHJhdmVyc2VGaWxlVHJlZShlLmRhdGFUcmFuc2ZlciEuaXRlbXMpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBmaWxlczogRmlsZVtdID0gQXJyYXkucHJvdG90eXBlLnNsaWNlXG4gICAgICAgIC5jYWxsKGUuZGF0YVRyYW5zZmVyIS5maWxlcylcbiAgICAgICAgLmZpbHRlcigoZmlsZTogRmlsZSkgPT4gdGhpcy5hdHRyQWNjZXB0KGZpbGUsIHRoaXMub3B0aW9ucy5hY2NlcHQpKTtcbiAgICAgIGlmIChmaWxlcy5sZW5ndGgpIHtcbiAgICAgICAgdGhpcy51cGxvYWRGaWxlcyhmaWxlcyk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICB9XG5cbiAgb25DaGFuZ2UoZTogRXZlbnQpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5vcHRpb25zLmRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNvbnN0IGhpZSA9IGUudGFyZ2V0IGFzIEhUTUxJbnB1dEVsZW1lbnQ7XG4gICAgdGhpcy51cGxvYWRGaWxlcyhoaWUuZmlsZXMhKTtcbiAgICBoaWUudmFsdWUgPSAnJztcbiAgfVxuXG4gIHByaXZhdGUgdHJhdmVyc2VGaWxlVHJlZShmaWxlczogRGF0YVRyYW5zZmVySXRlbUxpc3QpOiB2b2lkIHtcbiAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgY29uc3QgX3RyYXZlcnNlRmlsZVRyZWUgPSAoaXRlbTogYW55LCBwYXRoOiBzdHJpbmcpID0+IHtcbiAgICAgIGlmIChpdGVtLmlzRmlsZSkge1xuICAgICAgICBpdGVtLmZpbGUoKGZpbGU6IEZpbGUpID0+IHtcbiAgICAgICAgICBpZiAodGhpcy5hdHRyQWNjZXB0KGZpbGUsIHRoaXMub3B0aW9ucy5hY2NlcHQpKSB7XG4gICAgICAgICAgICB0aGlzLnVwbG9hZEZpbGVzKFtmaWxlXSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSBpZiAoaXRlbS5pc0RpcmVjdG9yeSkge1xuICAgICAgICBjb25zdCBkaXJSZWFkZXIgPSBpdGVtLmNyZWF0ZVJlYWRlcigpO1xuXG4gICAgICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgICAgICAgZGlyUmVhZGVyLnJlYWRFbnRyaWVzKChlbnRyaWVzOiBhbnkpID0+IHtcbiAgICAgICAgICBmb3IgKGNvbnN0IGVudHJpZUl0ZW0gb2YgZW50cmllcykge1xuICAgICAgICAgICAgX3RyYXZlcnNlRmlsZVRyZWUoZW50cmllSXRlbSwgYCR7cGF0aH0ke2l0ZW0ubmFtZX0vYCk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9O1xuICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgICBmb3IgKGNvbnN0IGZpbGUgb2YgZmlsZXMgYXMgYW55KSB7XG4gICAgICBfdHJhdmVyc2VGaWxlVHJlZShmaWxlLndlYmtpdEdldEFzRW50cnkoKSwgJycpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgYXR0ckFjY2VwdChmaWxlOiBGaWxlLCBhY2NlcHRlZEZpbGVzPzogc3RyaW5nIHwgc3RyaW5nW10pOiBib29sZWFuIHtcbiAgICBpZiAoZmlsZSAmJiBhY2NlcHRlZEZpbGVzKSB7XG4gICAgICBjb25zdCBhY2NlcHRlZEZpbGVzQXJyYXkgPSBBcnJheS5pc0FycmF5KGFjY2VwdGVkRmlsZXMpID8gYWNjZXB0ZWRGaWxlcyA6IGFjY2VwdGVkRmlsZXMuc3BsaXQoJywnKTtcbiAgICAgIGNvbnN0IGZpbGVOYW1lID0gJycgKyBmaWxlLm5hbWU7XG4gICAgICBjb25zdCBtaW1lVHlwZSA9ICcnICsgZmlsZS50eXBlO1xuICAgICAgY29uc3QgYmFzZU1pbWVUeXBlID0gbWltZVR5cGUucmVwbGFjZSgvXFwvLiokLywgJycpO1xuXG4gICAgICByZXR1cm4gYWNjZXB0ZWRGaWxlc0FycmF5LnNvbWUodHlwZSA9PiB7XG4gICAgICAgIGNvbnN0IHZhbGlkVHlwZSA9IHR5cGUudHJpbSgpO1xuICAgICAgICBpZiAodmFsaWRUeXBlLmNoYXJBdCgwKSA9PT0gJy4nKSB7XG4gICAgICAgICAgcmV0dXJuIChcbiAgICAgICAgICAgIGZpbGVOYW1lXG4gICAgICAgICAgICAgIC50b0xvd2VyQ2FzZSgpXG4gICAgICAgICAgICAgIC5pbmRleE9mKHZhbGlkVHlwZS50b0xvd2VyQ2FzZSgpLCBmaWxlTmFtZS50b0xvd2VyQ2FzZSgpLmxlbmd0aCAtIHZhbGlkVHlwZS50b0xvd2VyQ2FzZSgpLmxlbmd0aCkgIT09IC0xXG4gICAgICAgICAgKTtcbiAgICAgICAgfSBlbHNlIGlmICgvXFwvXFwqJC8udGVzdCh2YWxpZFR5cGUpKSB7XG4gICAgICAgICAgLy8gVGhpcyBpcyBzb21ldGhpbmcgbGlrZSBhIGltYWdlLyogbWltZSB0eXBlXG4gICAgICAgICAgcmV0dXJuIGJhc2VNaW1lVHlwZSA9PT0gdmFsaWRUeXBlLnJlcGxhY2UoL1xcLy4qJC8sICcnKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gbWltZVR5cGUgPT09IHZhbGlkVHlwZTtcbiAgICAgIH0pO1xuICAgIH1cbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHByaXZhdGUgYXR0YWNoVWlkKGZpbGU6IFVwbG9hZEZpbGUpOiBVcGxvYWRGaWxlIHtcbiAgICBpZiAoIWZpbGUudWlkKSB7XG4gICAgICBmaWxlLnVpZCA9IE1hdGgucmFuZG9tKClcbiAgICAgICAgLnRvU3RyaW5nKDM2KVxuICAgICAgICAuc3Vic3RyaW5nKDIpO1xuICAgIH1cbiAgICByZXR1cm4gZmlsZTtcbiAgfVxuXG4gIHVwbG9hZEZpbGVzKGZpbGVMaXN0OiBGaWxlTGlzdCB8IEZpbGVbXSk6IHZvaWQge1xuICAgIGxldCBmaWx0ZXJzJDogT2JzZXJ2YWJsZTxVcGxvYWRGaWxlW10+ID0gb2YoQXJyYXkucHJvdG90eXBlLnNsaWNlLmNhbGwoZmlsZUxpc3QpKTtcbiAgICBpZiAodGhpcy5vcHRpb25zLmZpbHRlcnMpIHtcbiAgICAgIHRoaXMub3B0aW9ucy5maWx0ZXJzLmZvckVhY2goZiA9PiB7XG4gICAgICAgIGZpbHRlcnMkID0gZmlsdGVycyQucGlwZShcbiAgICAgICAgICBzd2l0Y2hNYXAobGlzdCA9PiB7XG4gICAgICAgICAgICBjb25zdCBmblJlcyA9IGYuZm4obGlzdCk7XG4gICAgICAgICAgICByZXR1cm4gZm5SZXMgaW5zdGFuY2VvZiBPYnNlcnZhYmxlID8gZm5SZXMgOiBvZihmblJlcyk7XG4gICAgICAgICAgfSlcbiAgICAgICAgKTtcbiAgICAgIH0pO1xuICAgIH1cbiAgICBmaWx0ZXJzJC5zdWJzY3JpYmUoXG4gICAgICBsaXN0ID0+IHtcbiAgICAgICAgbGlzdC5mb3JFYWNoKChmaWxlOiBVcGxvYWRGaWxlKSA9PiB7XG4gICAgICAgICAgdGhpcy5hdHRhY2hVaWQoZmlsZSk7XG4gICAgICAgICAgdGhpcy51cGxvYWQoZmlsZSwgbGlzdCk7XG4gICAgICAgIH0pO1xuICAgICAgfSxcbiAgICAgIGUgPT4ge1xuICAgICAgICBjb25zb2xlLndhcm4oYFVuaGFuZGxlZCB1cGxvYWQgZmlsdGVyIGVycm9yYCwgZSk7XG4gICAgICB9XG4gICAgKTtcbiAgfVxuXG4gIHByaXZhdGUgdXBsb2FkKGZpbGU6IFVwbG9hZEZpbGUsIGZpbGVMaXN0OiBVcGxvYWRGaWxlW10pOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMub3B0aW9ucy5iZWZvcmVVcGxvYWQpIHtcbiAgICAgIHJldHVybiB0aGlzLnBvc3QoZmlsZSk7XG4gICAgfVxuICAgIGNvbnN0IGJlZm9yZSA9IHRoaXMub3B0aW9ucy5iZWZvcmVVcGxvYWQoZmlsZSwgZmlsZUxpc3QpO1xuICAgIGlmIChiZWZvcmUgaW5zdGFuY2VvZiBPYnNlcnZhYmxlKSB7XG4gICAgICBiZWZvcmUuc3Vic2NyaWJlKFxuICAgICAgICAocHJvY2Vzc2VkRmlsZTogVXBsb2FkRmlsZSkgPT4ge1xuICAgICAgICAgIGNvbnN0IHByb2Nlc3NlZEZpbGVUeXBlID0gT2JqZWN0LnByb3RvdHlwZS50b1N0cmluZy5jYWxsKHByb2Nlc3NlZEZpbGUpO1xuICAgICAgICAgIGlmIChwcm9jZXNzZWRGaWxlVHlwZSA9PT0gJ1tvYmplY3QgRmlsZV0nIHx8IHByb2Nlc3NlZEZpbGVUeXBlID09PSAnW29iamVjdCBCbG9iXScpIHtcbiAgICAgICAgICAgIHRoaXMuYXR0YWNoVWlkKHByb2Nlc3NlZEZpbGUpO1xuICAgICAgICAgICAgdGhpcy5wb3N0KHByb2Nlc3NlZEZpbGUpO1xuICAgICAgICAgIH0gZWxzZSBpZiAodHlwZW9mIHByb2Nlc3NlZEZpbGUgPT09ICdib29sZWFuJyAmJiBwcm9jZXNzZWRGaWxlICE9PSBmYWxzZSkge1xuICAgICAgICAgICAgdGhpcy5wb3N0KGZpbGUpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSxcbiAgICAgICAgZSA9PiB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGBVbmhhbmRsZWQgdXBsb2FkIGJlZm9yZVVwbG9hZCBlcnJvcmAsIGUpO1xuICAgICAgICB9XG4gICAgICApO1xuICAgIH0gZWxzZSBpZiAoYmVmb3JlICE9PSBmYWxzZSkge1xuICAgICAgcmV0dXJuIHRoaXMucG9zdChmaWxlKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHBvc3QoZmlsZTogVXBsb2FkRmlsZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLmRlc3Ryb3kpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgY29uc3Qgb3B0ID0gdGhpcy5vcHRpb25zO1xuICAgIGNvbnN0IHsgdWlkIH0gPSBmaWxlO1xuICAgIGxldCB7IGRhdGEsIGhlYWRlcnMgfSA9IG9wdDtcbiAgICBpZiAodHlwZW9mIGRhdGEgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgIGRhdGEgPSAoZGF0YSBhcyAoZmlsZTogVXBsb2FkRmlsZSkgPT4ge30pKGZpbGUpO1xuICAgIH1cbiAgICBpZiAodHlwZW9mIGhlYWRlcnMgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgIGhlYWRlcnMgPSAoaGVhZGVycyBhcyAoZmlsZTogVXBsb2FkRmlsZSkgPT4ge30pKGZpbGUpO1xuICAgIH1cbiAgICBjb25zdCBhcmdzOiBVcGxvYWRYSFJBcmdzID0ge1xuICAgICAgYWN0aW9uOiBvcHQuYWN0aW9uLFxuICAgICAgbmFtZTogb3B0Lm5hbWUsXG4gICAgICBoZWFkZXJzLFxuICAgICAgZmlsZSxcbiAgICAgIGRhdGEsXG4gICAgICB3aXRoQ3JlZGVudGlhbHM6IG9wdC53aXRoQ3JlZGVudGlhbHMsXG4gICAgICBvblByb2dyZXNzOiBvcHQub25Qcm9ncmVzc1xuICAgICAgICA/IGUgPT4ge1xuICAgICAgICAgICAgb3B0Lm9uUHJvZ3Jlc3MhKGUsIGZpbGUpO1xuICAgICAgICAgIH1cbiAgICAgICAgOiB1bmRlZmluZWQsXG4gICAgICBvblN1Y2Nlc3M6IChyZXQsIHhocikgPT4ge1xuICAgICAgICB0aGlzLmNsZWFuKHVpZCk7XG4gICAgICAgIG9wdC5vblN1Y2Nlc3MhKHJldCwgZmlsZSwgeGhyKTtcbiAgICAgIH0sXG4gICAgICBvbkVycm9yOiB4aHIgPT4ge1xuICAgICAgICB0aGlzLmNsZWFuKHVpZCk7XG4gICAgICAgIG9wdC5vbkVycm9yISh4aHIsIGZpbGUpO1xuICAgICAgfVxuICAgIH07XG4gICAgY29uc3QgcmVxJCA9IChvcHQuY3VzdG9tUmVxdWVzdCB8fCB0aGlzLnhocikuY2FsbCh0aGlzLCBhcmdzKTtcbiAgICBpZiAoIShyZXEkIGluc3RhbmNlb2YgU3Vic2NyaXB0aW9uKSkge1xuICAgICAgY29uc29sZS53YXJuKGBNdXN0IHJldHVybiBTdWJzY3JpcHRpb24gdHlwZSBpbiAnW256Q3VzdG9tUmVxdWVzdF0nIHByb3BlcnR5YCk7XG4gICAgfVxuICAgIHRoaXMucmVxc1t1aWRdID0gcmVxJDtcbiAgICBvcHQub25TdGFydCEoZmlsZSk7XG4gIH1cblxuICBwcml2YXRlIHhocihhcmdzOiBVcGxvYWRYSFJBcmdzKTogU3Vic2NyaXB0aW9uIHtcbiAgICBjb25zdCBmb3JtRGF0YSA9IG5ldyBGb3JtRGF0YSgpO1xuICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgICBmb3JtRGF0YS5hcHBlbmQoYXJncy5uYW1lISwgYXJncy5maWxlIGFzIGFueSk7XG4gICAgaWYgKGFyZ3MuZGF0YSkge1xuICAgICAgT2JqZWN0LmtleXMoYXJncy5kYXRhKS5tYXAoa2V5ID0+IHtcbiAgICAgICAgZm9ybURhdGEuYXBwZW5kKGtleSwgYXJncy5kYXRhIVtrZXldKTtcbiAgICAgIH0pO1xuICAgIH1cbiAgICBpZiAoIWFyZ3MuaGVhZGVycykge1xuICAgICAgYXJncy5oZWFkZXJzID0ge307XG4gICAgfVxuICAgIGlmIChhcmdzLmhlYWRlcnNbJ1gtUmVxdWVzdGVkLVdpdGgnXSAhPT0gbnVsbCkge1xuICAgICAgYXJncy5oZWFkZXJzWydYLVJlcXVlc3RlZC1XaXRoJ10gPSBgWE1MSHR0cFJlcXVlc3RgO1xuICAgIH0gZWxzZSB7XG4gICAgICBkZWxldGUgYXJncy5oZWFkZXJzWydYLVJlcXVlc3RlZC1XaXRoJ107XG4gICAgfVxuICAgIGNvbnN0IHJlcSA9IG5ldyBIdHRwUmVxdWVzdCgnUE9TVCcsIGFyZ3MuYWN0aW9uISwgZm9ybURhdGEsIHtcbiAgICAgIHJlcG9ydFByb2dyZXNzOiB0cnVlLFxuICAgICAgd2l0aENyZWRlbnRpYWxzOiBhcmdzLndpdGhDcmVkZW50aWFscyxcbiAgICAgIGhlYWRlcnM6IG5ldyBIdHRwSGVhZGVycyhhcmdzLmhlYWRlcnMpXG4gICAgfSk7XG4gICAgcmV0dXJuIHRoaXMuaHR0cC5yZXF1ZXN0KHJlcSkuc3Vic2NyaWJlKFxuICAgICAgKGV2ZW50OiBIdHRwRXZlbnQ8e30+KSA9PiB7XG4gICAgICAgIGlmIChldmVudC50eXBlID09PSBIdHRwRXZlbnRUeXBlLlVwbG9hZFByb2dyZXNzKSB7XG4gICAgICAgICAgaWYgKGV2ZW50LnRvdGFsISA+IDApIHtcbiAgICAgICAgICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgICAgICAgICAgIChldmVudCBhcyBhbnkpLnBlcmNlbnQgPSAoZXZlbnQubG9hZGVkIC8gZXZlbnQudG90YWwhKSAqIDEwMDtcbiAgICAgICAgICB9XG4gICAgICAgICAgYXJncy5vblByb2dyZXNzIShldmVudCwgYXJncy5maWxlKTtcbiAgICAgICAgfSBlbHNlIGlmIChldmVudCBpbnN0YW5jZW9mIEh0dHBSZXNwb25zZSkge1xuICAgICAgICAgIGFyZ3Mub25TdWNjZXNzIShldmVudC5ib2R5LCBhcmdzLmZpbGUsIGV2ZW50KTtcbiAgICAgICAgfVxuICAgICAgfSxcbiAgICAgIGVyciA9PiB7XG4gICAgICAgIHRoaXMuYWJvcnQoYXJncy5maWxlKTtcbiAgICAgICAgYXJncy5vbkVycm9yIShlcnIsIGFyZ3MuZmlsZSk7XG4gICAgICB9XG4gICAgKTtcbiAgfVxuXG4gIHByaXZhdGUgY2xlYW4odWlkOiBzdHJpbmcpOiB2b2lkIHtcbiAgICBjb25zdCByZXEkID0gdGhpcy5yZXFzW3VpZF07XG4gICAgaWYgKHJlcSQgaW5zdGFuY2VvZiBTdWJzY3JpcHRpb24pIHtcbiAgICAgIHJlcSQudW5zdWJzY3JpYmUoKTtcbiAgICB9XG4gICAgZGVsZXRlIHRoaXMucmVxc1t1aWRdO1xuICB9XG5cbiAgYWJvcnQoZmlsZT86IFVwbG9hZEZpbGUpOiB2b2lkIHtcbiAgICBpZiAoZmlsZSkge1xuICAgICAgdGhpcy5jbGVhbihmaWxlICYmIGZpbGUudWlkKTtcbiAgICB9IGVsc2Uge1xuICAgICAgT2JqZWN0LmtleXModGhpcy5yZXFzKS5mb3JFYWNoKHVpZCA9PiB0aGlzLmNsZWFuKHVpZCkpO1xuICAgIH1cbiAgfVxuXG4gIC8vICNyZWdpb24gc3R5bGVzXG5cbiAgcHJpdmF0ZSBwcmVmaXhDbHMgPSAnYW50LXVwbG9hZCc7XG5cbiAgcHJpdmF0ZSBzZXRDbGFzc01hcCgpOiB2b2lkIHtcbiAgICBjb25zdCBjbGFzc01hcCA9IHtcbiAgICAgIFt0aGlzLnByZWZpeENsc106IHRydWUsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LWRpc2FibGVkYF06IHRoaXMub3B0aW9ucy5kaXNhYmxlZCxcbiAgICAgIC4uLnRoaXMuY2xhc3Nlc1xuICAgIH07XG4gICAgdGhpcy51cGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLnVwZGF0ZUhvc3RDbGFzcyh0aGlzLmVsLm5hdGl2ZUVsZW1lbnQsIGNsYXNzTWFwKTtcbiAgfVxuXG4gIC8vICNlbmRyZWdpb25cblxuICBjb25zdHJ1Y3RvcihcbiAgICBAT3B0aW9uYWwoKSBwcml2YXRlIGh0dHA6IEh0dHBDbGllbnQsXG4gICAgcHJpdmF0ZSBlbDogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIHVwZGF0ZUhvc3RDbGFzc1NlcnZpY2U6IE56VXBkYXRlSG9zdENsYXNzU2VydmljZVxuICApIHtcbiAgICBpZiAoIWh0dHApIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgTm90IGZvdW5kICdIdHRwQ2xpZW50JywgWW91IGNhbiBpbXBvcnQgJ0h0dHBDbGllbnRNb2R1bGUnIGluIHlvdXIgcm9vdCBtb2R1bGUuYCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5pbml0ZWQgPSB0cnVlO1xuICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmluaXRlZCkge1xuICAgICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveSA9IHRydWU7XG4gICAgdGhpcy5hYm9ydCgpO1xuICB9XG59XG4iXX0=