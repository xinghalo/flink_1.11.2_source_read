/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ENTER } from '@angular/cdk/keycodes';
import { HttpClient, HttpEventType, HttpHeaders, HttpRequest, HttpResponse } from '@angular/common/http';
import { Component, ElementRef, HostListener, Input, Optional, ViewChild, ViewEncapsulation } from '@angular/core';
import { of, Observable, Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
export class NzUploadBtnComponent {
    // #endregion
    /**
     * @param {?} http
     * @param {?} el
     * @param {?} updateHostClassService
     */
    constructor(http, el, updateHostClassService) {
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
            throw new Error(`Not found 'HttpClient', You can import 'HttpClientModule' in your root module.`);
        }
    }
    // #endregion
    /**
     * @return {?}
     */
    onClick() {
        if (this.options.disabled || !this.options.openFileDialogOnClick) {
            return;
        }
        ((/** @type {?} */ (this.file.nativeElement))).click();
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onKeyDown(e) {
        if (this.options.disabled) {
            return;
        }
        if (e.key === 'Enter' || e.keyCode === ENTER) {
            this.onClick();
        }
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onFileDrop(e) {
        if (this.options.disabled || e.type === 'dragover') {
            e.preventDefault();
            return;
        }
        if (this.options.directory) {
            this.traverseFileTree((/** @type {?} */ (e.dataTransfer)).items);
        }
        else {
            /** @type {?} */
            const files = Array.prototype.slice
                .call((/** @type {?} */ (e.dataTransfer)).files)
                .filter((/**
             * @param {?} file
             * @return {?}
             */
            (file) => this.attrAccept(file, this.options.accept)));
            if (files.length) {
                this.uploadFiles(files);
            }
        }
        e.preventDefault();
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onChange(e) {
        if (this.options.disabled) {
            return;
        }
        /** @type {?} */
        const hie = (/** @type {?} */ (e.target));
        this.uploadFiles((/** @type {?} */ (hie.files)));
        hie.value = '';
    }
    /**
     * @private
     * @param {?} files
     * @return {?}
     */
    traverseFileTree(files) {
        // tslint:disable-next-line:no-any
        /** @type {?} */
        const _traverseFileTree = (/**
         * @param {?} item
         * @param {?} path
         * @return {?}
         */
        (item, path) => {
            if (item.isFile) {
                item.file((/**
                 * @param {?} file
                 * @return {?}
                 */
                (file) => {
                    if (this.attrAccept(file, this.options.accept)) {
                        this.uploadFiles([file]);
                    }
                }));
            }
            else if (item.isDirectory) {
                /** @type {?} */
                const dirReader = item.createReader();
                // tslint:disable-next-line:no-any
                dirReader.readEntries((/**
                 * @param {?} entries
                 * @return {?}
                 */
                (entries) => {
                    for (const entrieItem of entries) {
                        _traverseFileTree(entrieItem, `${path}${item.name}/`);
                    }
                }));
            }
        });
        // tslint:disable-next-line:no-any
        for (const file of (/** @type {?} */ (files))) {
            _traverseFileTree(file.webkitGetAsEntry(), '');
        }
    }
    /**
     * @private
     * @param {?} file
     * @param {?=} acceptedFiles
     * @return {?}
     */
    attrAccept(file, acceptedFiles) {
        if (file && acceptedFiles) {
            /** @type {?} */
            const acceptedFilesArray = Array.isArray(acceptedFiles) ? acceptedFiles : acceptedFiles.split(',');
            /** @type {?} */
            const fileName = '' + file.name;
            /** @type {?} */
            const mimeType = '' + file.type;
            /** @type {?} */
            const baseMimeType = mimeType.replace(/\/.*$/, '');
            return acceptedFilesArray.some((/**
             * @param {?} type
             * @return {?}
             */
            type => {
                /** @type {?} */
                const validType = type.trim();
                if (validType.charAt(0) === '.') {
                    return (fileName
                        .toLowerCase()
                        .indexOf(validType.toLowerCase(), fileName.toLowerCase().length - validType.toLowerCase().length) !== -1);
                }
                else if (/\/\*$/.test(validType)) {
                    // This is something like a image/* mime type
                    return baseMimeType === validType.replace(/\/.*$/, '');
                }
                return mimeType === validType;
            }));
        }
        return true;
    }
    /**
     * @private
     * @param {?} file
     * @return {?}
     */
    attachUid(file) {
        if (!file.uid) {
            file.uid = Math.random()
                .toString(36)
                .substring(2);
        }
        return file;
    }
    /**
     * @param {?} fileList
     * @return {?}
     */
    uploadFiles(fileList) {
        /** @type {?} */
        let filters$ = of(Array.prototype.slice.call(fileList));
        if (this.options.filters) {
            this.options.filters.forEach((/**
             * @param {?} f
             * @return {?}
             */
            f => {
                filters$ = filters$.pipe(switchMap((/**
                 * @param {?} list
                 * @return {?}
                 */
                list => {
                    /** @type {?} */
                    const fnRes = f.fn(list);
                    return fnRes instanceof Observable ? fnRes : of(fnRes);
                })));
            }));
        }
        filters$.subscribe((/**
         * @param {?} list
         * @return {?}
         */
        list => {
            list.forEach((/**
             * @param {?} file
             * @return {?}
             */
            (file) => {
                this.attachUid(file);
                this.upload(file, list);
            }));
        }), (/**
         * @param {?} e
         * @return {?}
         */
        e => {
            console.warn(`Unhandled upload filter error`, e);
        }));
    }
    /**
     * @private
     * @param {?} file
     * @param {?} fileList
     * @return {?}
     */
    upload(file, fileList) {
        if (!this.options.beforeUpload) {
            return this.post(file);
        }
        /** @type {?} */
        const before = this.options.beforeUpload(file, fileList);
        if (before instanceof Observable) {
            before.subscribe((/**
             * @param {?} processedFile
             * @return {?}
             */
            (processedFile) => {
                /** @type {?} */
                const processedFileType = Object.prototype.toString.call(processedFile);
                if (processedFileType === '[object File]' || processedFileType === '[object Blob]') {
                    this.attachUid(processedFile);
                    this.post(processedFile);
                }
                else if (typeof processedFile === 'boolean' && processedFile !== false) {
                    this.post(file);
                }
            }), (/**
             * @param {?} e
             * @return {?}
             */
            e => {
                console.warn(`Unhandled upload beforeUpload error`, e);
            }));
        }
        else if (before !== false) {
            return this.post(file);
        }
    }
    /**
     * @private
     * @param {?} file
     * @return {?}
     */
    post(file) {
        if (this.destroy) {
            return;
        }
        /** @type {?} */
        const opt = this.options;
        const { uid } = file;
        let { data, headers } = opt;
        if (typeof data === 'function') {
            data = ((/** @type {?} */ (data)))(file);
        }
        if (typeof headers === 'function') {
            headers = ((/** @type {?} */ (headers)))(file);
        }
        /** @type {?} */
        const args = {
            action: opt.action,
            name: opt.name,
            headers,
            file,
            data,
            withCredentials: opt.withCredentials,
            onProgress: opt.onProgress
                ? (/**
                 * @param {?} e
                 * @return {?}
                 */
                e => {
                    (/** @type {?} */ (opt.onProgress))(e, file);
                })
                : undefined,
            onSuccess: (/**
             * @param {?} ret
             * @param {?} xhr
             * @return {?}
             */
            (ret, xhr) => {
                this.clean(uid);
                (/** @type {?} */ (opt.onSuccess))(ret, file, xhr);
            }),
            onError: (/**
             * @param {?} xhr
             * @return {?}
             */
            xhr => {
                this.clean(uid);
                (/** @type {?} */ (opt.onError))(xhr, file);
            })
        };
        /** @type {?} */
        const req$ = (opt.customRequest || this.xhr).call(this, args);
        if (!(req$ instanceof Subscription)) {
            console.warn(`Must return Subscription type in '[nzCustomRequest]' property`);
        }
        this.reqs[uid] = req$;
        (/** @type {?} */ (opt.onStart))(file);
    }
    /**
     * @private
     * @param {?} args
     * @return {?}
     */
    xhr(args) {
        /** @type {?} */
        const formData = new FormData();
        // tslint:disable-next-line:no-any
        formData.append((/** @type {?} */ (args.name)), (/** @type {?} */ (args.file)));
        if (args.data) {
            Object.keys(args.data).map((/**
             * @param {?} key
             * @return {?}
             */
            key => {
                formData.append(key, (/** @type {?} */ (args.data))[key]);
            }));
        }
        if (!args.headers) {
            args.headers = {};
        }
        if (args.headers['X-Requested-With'] !== null) {
            args.headers['X-Requested-With'] = `XMLHttpRequest`;
        }
        else {
            delete args.headers['X-Requested-With'];
        }
        /** @type {?} */
        const req = new HttpRequest('POST', (/** @type {?} */ (args.action)), formData, {
            reportProgress: true,
            withCredentials: args.withCredentials,
            headers: new HttpHeaders(args.headers)
        });
        return this.http.request(req).subscribe((/**
         * @param {?} event
         * @return {?}
         */
        (event) => {
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
        err => {
            this.abort(args.file);
            (/** @type {?} */ (args.onError))(err, args.file);
        }));
    }
    /**
     * @private
     * @param {?} uid
     * @return {?}
     */
    clean(uid) {
        /** @type {?} */
        const req$ = this.reqs[uid];
        if (req$ instanceof Subscription) {
            req$.unsubscribe();
        }
        delete this.reqs[uid];
    }
    /**
     * @param {?=} file
     * @return {?}
     */
    abort(file) {
        if (file) {
            this.clean(file && file.uid);
        }
        else {
            Object.keys(this.reqs).forEach((/**
             * @param {?} uid
             * @return {?}
             */
            uid => this.clean(uid)));
        }
    }
    /**
     * @private
     * @return {?}
     */
    setClassMap() {
        /** @type {?} */
        const classMap = Object.assign({ [this.prefixCls]: true, [`${this.prefixCls}-disabled`]: this.options.disabled }, this.classes);
        this.updateHostClassService.updateHostClass(this.el.nativeElement, classMap);
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.inited = true;
        this.setClassMap();
    }
    /**
     * @return {?}
     */
    ngOnChanges() {
        if (this.inited) {
            this.setClassMap();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy = true;
        this.abort();
    }
}
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
NzUploadBtnComponent.ctorParameters = () => [
    { type: HttpClient, decorators: [{ type: Optional }] },
    { type: ElementRef },
    { type: NzUpdateHostClassService }
];
NzUploadBtnComponent.propDecorators = {
    file: [{ type: ViewChild, args: ['file',] }],
    classes: [{ type: Input }],
    options: [{ type: Input }],
    onClick: [{ type: HostListener, args: ['click',] }],
    onKeyDown: [{ type: HostListener, args: ['keydown', ['$event'],] }],
    onFileDrop: [{ type: HostListener, args: ['drop', ['$event'],] }, { type: HostListener, args: ['dragover', ['$event'],] }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdXBsb2FkLWJ0bi5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidXBsb2FkL256LXVwbG9hZC1idG4uY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsS0FBSyxFQUFFLE1BQU0sdUJBQXVCLENBQUM7QUFDOUMsT0FBTyxFQUFFLFVBQVUsRUFBYSxhQUFhLEVBQUUsV0FBVyxFQUFFLFdBQVcsRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwSCxPQUFPLEVBQ0wsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUlMLFFBQVEsRUFDUixTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxFQUFFLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUNwRCxPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFM0MsT0FBTyxFQUFFLHdCQUF3QixFQUFFLE1BQU0sNENBQTRDLENBQUM7QUFldEYsTUFBTSxPQUFPLG9CQUFvQjs7Ozs7OztJQTZSL0IsWUFDc0IsSUFBZ0IsRUFDNUIsRUFBYyxFQUNkLHNCQUFnRDtRQUZwQyxTQUFJLEdBQUosSUFBSSxDQUFZO1FBQzVCLE9BQUUsR0FBRixFQUFFLENBQVk7UUFDZCwyQkFBc0IsR0FBdEIsc0JBQXNCLENBQTBCO1FBL1IxRCxTQUFJLEdBQW9DLEVBQUUsQ0FBQztRQUNuQyxXQUFNLEdBQUcsS0FBSyxDQUFDO1FBQ2YsWUFBTyxHQUFHLEtBQUssQ0FBQzs7UUFLZixZQUFPLEdBQU8sRUFBRSxDQUFDOztRQXdRbEIsY0FBUyxHQUFHLFlBQVksQ0FBQztRQWtCL0IsSUFBSSxDQUFDLElBQUksRUFBRTtZQUNULE1BQU0sSUFBSSxLQUFLLENBQUMsZ0ZBQWdGLENBQUMsQ0FBQztTQUNuRztJ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QnBDLG9DQUEyQzs7Ozs7SUFDM0Msc0NBQXVCOzs7OztJQUN2Qix1Q0FBd0I7O0lBRXhCLG9DQUFvQzs7SUFHcEMsdUNBQTBCOztJQUMxQix1Q0FBbUM7Ozs7O0lBdVFuQyx5Q0FBaUM7Ozs7O0lBYy9CLG9DQUFvQzs7Ozs7SUFDcEMsa0NBQXNCOzs7OztJQUN0QixzREFBd0QiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBFTlRFUiB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9rZXljb2Rlcyc7XG5pbXBvcnQgeyBIdHRwQ2xpZW50LCBIdHRwRXZlbnQsIEh0dHBFdmVudFR5cGUsIEh0dHBIZWFkZXJzLCBIdHRwUmVxdWVzdCwgSHR0cFJlc3BvbnNlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uL2h0dHAnO1xuaW1wb3J0IHtcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBIb3N0TGlzdGVuZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uRGVzdHJveSxcbiAgT25Jbml0LFxuICBPcHRpb25hbCxcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IG9mLCBPYnNlcnZhYmxlLCBTdWJzY3JpcHRpb24gfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IHN3aXRjaE1hcCB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcblxuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcblxuaW1wb3J0IHsgVXBsb2FkRmlsZSwgVXBsb2FkWEhSQXJncywgWmlwQnV0dG9uT3B0aW9ucyB9IGZyb20gJy4vaW50ZXJmYWNlJztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnW256LXVwbG9hZC1idG5dJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LXVwbG9hZC1idG4uY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1thdHRyLnRhYmluZGV4XSc6ICdcIjBcIicsXG4gICAgJ1thdHRyLnJvbGVdJzogJ1wiYnV0dG9uXCInXG4gIH0sXG4gIHByb3ZpZGVyczogW056VXBkYXRlSG9zdENsYXNzU2VydmljZV0sXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lXG59KVxuZXhwb3J0IGNsYXNzIE56VXBsb2FkQnRuQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkNoYW5nZXMsIE9uRGVzdHJveSB7XG4gIHJlcXM6IHsgW2tleTogc3RyaW5nXTogU3Vic2NyaXB0aW9uIH0gPSB7fTtcbiAgcHJpdmF0ZSBpbml0ZWQgPSBmYWxzZTtcbiAgcHJpdmF0ZSBkZXN0cm95ID0gZmFsc2U7XG5cbiAgQFZpZXdDaGlsZCgnZmlsZScpIGZpbGU6IEVsZW1lbnRSZWY7XG5cbiAgLy8gI3JlZ2lvbiBmaWVsZHNcbiAgQElucHV0KCkgY2xhc3Nlczoge30gPSB7fTtcbiAgQElucHV0KCkgb3B0aW9uczogWmlwQnV0dG9uT3B0aW9ucztcblxuICAvLyAjZW5kcmVnaW9uXG5cbiAgQEhvc3RMaXN0ZW5lcignY2xpY2snKVxuICBvbkNsaWNrKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm9wdGlvbnMuZGlzYWJsZWQgfHwgIXRoaXMub3B0aW9ucy5vcGVuRmlsZURpYWxvZ09uQ2xpY2spIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgKHRoaXMuZmlsZS5uYXRpdmVFbGVtZW50IGFzIEhUTUxJbnB1dEVsZW1lbnQpLmNsaWNrKCk7XG4gIH1cblxuICBASG9zdExpc3RlbmVyKCdrZXlkb3duJywgWyckZXZlbnQnXSlcbiAgb25LZXlEb3duKGU6IEtleWJvYXJkRXZlbnQpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5vcHRpb25zLmRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmIChlLmtleSA9PT0gJ0VudGVyJyB8fCBlLmtleUNvZGUgPT09IEVOVEVSKSB7XG4gICAgICB0aGlzLm9uQ2xpY2soKTtcbiAgICB9XG4gIH1cblxuICBASG9zdExpc3RlbmVyKCdkcm9wJywgWyckZXZlbnQnXSlcbiAgQEhvc3RMaXN0ZW5lcignZHJhZ292ZXInLCBbJyRldmVudCddKVxuICBvbkZpbGVEcm9wKGU6IERyYWdFdmVudCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm9wdGlvbnMuZGlzYWJsZWQgfHwgZS50eXBlID09PSAnZHJhZ292ZXInKSB7XG4gICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmICh0aGlzLm9wdGlvbnMuZGlyZWN0b3J5KSB7XG4gICAgICB0aGlzLnRyYXZlcnNlRmlsZVRyZWUoZS5kYXRhVHJhbnNmZXIhLml0ZW1zKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgZmlsZXM6IEZpbGVbXSA9IEFycmF5LnByb3RvdHlwZS5zbGljZVxuICAgICAgICAuY2FsbChlLmRhdGFUcmFuc2ZlciEuZmlsZXMpXG4gICAgICAgIC5maWx0ZXIoKGZpbGU6IEZpbGUpID0+IHRoaXMuYXR0ckFjY2VwdChmaWxlLCB0aGlzLm9wdGlvbnMuYWNjZXB0KSk7XG4gICAgICBpZiAoZmlsZXMubGVuZ3RoKSB7XG4gICAgICAgIHRoaXMudXBsb2FkRmlsZXMoZmlsZXMpO1xuICAgICAgfVxuICAgIH1cblxuICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgfVxuXG4gIG9uQ2hhbmdlKGU6IEV2ZW50KTogdm9pZCB7XG4gICAgaWYgKHRoaXMub3B0aW9ucy5kaXNhYmxlZCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCBoaWUgPSBlLnRhcmdldCBhcyBIVE1MSW5wdXRFbGVtZW50O1xuICAgIHRoaXMudXBsb2FkRmlsZXMoaGllLmZpbGVzISk7XG4gICAgaGllLnZhbHVlID0gJyc7XG4gIH1cblxuICBwcml2YXRlIHRyYXZlcnNlRmlsZVRyZWUoZmlsZXM6IERhdGFUcmFuc2Zlckl0ZW1MaXN0KTogdm9pZCB7XG4gICAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICAgIGNvbnN0IF90cmF2ZXJzZUZpbGVUcmVlID0gKGl0ZW06IGFueSwgcGF0aDogc3RyaW5nKSA9PiB7XG4gICAgICBpZiAoaXRlbS5pc0ZpbGUpIHtcbiAgICAgICAgaXRlbS5maWxlKChmaWxlOiBGaWxlKSA9PiB7XG4gICAgICAgICAgaWYgKHRoaXMuYXR0ckFjY2VwdChmaWxlLCB0aGlzLm9wdGlvbnMuYWNjZXB0KSkge1xuICAgICAgICAgICAgdGhpcy51cGxvYWRGaWxlcyhbZmlsZV0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2UgaWYgKGl0ZW0uaXNEaXJlY3RvcnkpIHtcbiAgICAgICAgY29uc3QgZGlyUmVhZGVyID0gaXRlbS5jcmVhdGVSZWFkZXIoKTtcblxuICAgICAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgICAgIGRpclJlYWRlci5yZWFkRW50cmllcygoZW50cmllczogYW55KSA9PiB7XG4gICAgICAgICAgZm9yIChjb25zdCBlbnRyaWVJdGVtIG9mIGVudHJpZXMpIHtcbiAgICAgICAgICAgIF90cmF2ZXJzZUZpbGVUcmVlKGVudHJpZUl0ZW0sIGAke3BhdGh9JHtpdGVtLm5hbWV9L2ApO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfTtcbiAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgZm9yIChjb25zdCBmaWxlIG9mIGZpbGVzIGFzIGFueSkge1xuICAgICAgX3RyYXZlcnNlRmlsZVRyZWUoZmlsZS53ZWJraXRHZXRBc0VudHJ5KCksICcnKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGF0dHJBY2NlcHQoZmlsZTogRmlsZSwgYWNjZXB0ZWRGaWxlcz86IHN0cmluZyB8IHN0cmluZ1tdKTogYm9vbGVhbiB7XG4gICAgaWYgKGZpbGUgJiYgYWNjZXB0ZWRGaWxlcykge1xuICAgICAgY29uc3QgYWNjZXB0ZWRGaWxlc0FycmF5ID0gQXJyYXkuaXNBcnJheShhY2NlcHRlZEZpbGVzKSA/IGFjY2VwdGVkRmlsZXMgOiBhY2NlcHRlZEZpbGVzLnNwbGl0KCcsJyk7XG4gICAgICBjb25zdCBmaWxlTmFtZSA9ICcnICsgZmlsZS5uYW1lO1xuICAgICAgY29uc3QgbWltZVR5cGUgPSAnJyArIGZpbGUudHlwZTtcbiAgICAgIGNvbnN0IGJhc2VNaW1lVHlwZSA9IG1pbWVUeXBlLnJlcGxhY2UoL1xcLy4qJC8sICcnKTtcblxuICAgICAgcmV0dXJuIGFjY2VwdGVkRmlsZXNBcnJheS5zb21lKHR5cGUgPT4ge1xuICAgICAgICBjb25zdCB2YWxpZFR5cGUgPSB0eXBlLnRyaW0oKTtcbiAgICAgICAgaWYgKHZhbGlkVHlwZS5jaGFyQXQoMCkgPT09ICcuJykge1xuICAgICAgICAgIHJldHVybiAoXG4gICAgICAgICAgICBmaWxlTmFtZVxuICAgICAgICAgICAgICAudG9Mb3dlckNhc2UoKVxuICAgICAgICAgICAgICAuaW5kZXhPZih2YWxpZFR5cGUudG9Mb3dlckNhc2UoKSwgZmlsZU5hbWUudG9Mb3dlckNhc2UoKS5sZW5ndGggLSB2YWxpZFR5cGUudG9Mb3dlckNhc2UoKS5sZW5ndGgpICE9PSAtMVxuICAgICAgICAgICk7XG4gICAgICAgIH0gZWxzZSBpZiAoL1xcL1xcKiQvLnRlc3QodmFsaWRUeXBlKSkge1xuICAgICAgICAgIC8vIFRoaXMgaXMgc29tZXRoaW5nIGxpa2UgYSBpbWFnZS8qIG1pbWUgdHlwZVxuICAgICAgICAgIHJldHVybiBiYXNlTWltZVR5cGUgPT09IHZhbGlkVHlwZS5yZXBsYWNlKC9cXC8uKiQvLCAnJyk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIG1pbWVUeXBlID09PSB2YWxpZFR5cGU7XG4gICAgICB9KTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICBwcml2YXRlIGF0dGFjaFVpZChmaWxlOiBVcGxvYWRGaWxlKTogVXBsb2FkRmlsZSB7XG4gICAgaWYgKCFmaWxlLnVpZCkge1xuICAgICAgZmlsZS51aWQgPSBNYXRoLnJhbmRvbSgpXG4gICAgICAgIC50b1N0cmluZygzNilcbiAgICAgICAgLnN1YnN0cmluZygyKTtcbiAgICB9XG4gICAgcmV0dXJuIGZpbGU7XG4gIH1cblxuICB1cGxvYWRGaWxlcyhmaWxlTGlzdDogRmlsZUxpc3QgfCBGaWxlW10pOiB2b2lkIHtcbiAgICBsZXQgZmlsdGVycyQ6IE9ic2VydmFibGU8VXBsb2FkRmlsZVtdPiA9IG9mKEFycmF5LnByb3RvdHlwZS5zbGljZS5jYWxsKGZpbGVMaXN0KSk7XG4gICAgaWYgKHRoaXMub3B0aW9ucy5maWx0ZXJzKSB7XG4gICAgICB0aGlzLm9wdGlvbnMuZmlsdGVycy5mb3JFYWNoKGYgPT4ge1xuICAgICAgICBmaWx0ZXJzJCA9IGZpbHRlcnMkLnBpcGUoXG4gICAgICAgICAgc3dpdGNoTWFwKGxpc3QgPT4ge1xuICAgICAgICAgICAgY29uc3QgZm5SZXMgPSBmLmZuKGxpc3QpO1xuICAgICAgICAgICAgcmV0dXJuIGZuUmVzIGluc3RhbmNlb2YgT2JzZXJ2YWJsZSA/IGZuUmVzIDogb2YoZm5SZXMpO1xuICAgICAgICAgIH0pXG4gICAgICAgICk7XG4gICAgICB9KTtcbiAgICB9XG4gICAgZmlsdGVycyQuc3Vic2NyaWJlKFxuICAgICAgbGlzdCA9PiB7XG4gICAgICAgIGxpc3QuZm9yRWFjaCgoZmlsZTogVXBsb2FkRmlsZSkgPT4ge1xuICAgICAgICAgIHRoaXMuYXR0YWNoVWlkKGZpbGUpO1xuICAgICAgICAgIHRoaXMudXBsb2FkKGZpbGUsIGxpc3QpO1xuICAgICAgICB9KTtcbiAgICAgIH0sXG4gICAgICBlID0+IHtcbiAgICAgICAgY29uc29sZS53YXJuKGBVbmhhbmRsZWQgdXBsb2FkIGZpbHRlciBlcnJvcmAsIGUpO1xuICAgICAgfVxuICAgICk7XG4gIH1cblxuICBwcml2YXRlIHVwbG9hZChmaWxlOiBVcGxvYWRGaWxlLCBmaWxlTGlzdDogVXBsb2FkRmlsZVtdKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLm9wdGlvbnMuYmVmb3JlVXBsb2FkKSB7XG4gICAgICByZXR1cm4gdGhpcy5wb3N0KGZpbGUpO1xuICAgIH1cbiAgICBjb25zdCBiZWZvcmUgPSB0aGlzLm9wdGlvbnMuYmVmb3JlVXBsb2FkKGZpbGUsIGZpbGVMaXN0KTtcbiAgICBpZiAoYmVmb3JlIGluc3RhbmNlb2YgT2JzZXJ2YWJsZSkge1xuICAgICAgYmVmb3JlLnN1YnNjcmliZShcbiAgICAgICAgKHByb2Nlc3NlZEZpbGU6IFVwbG9hZEZpbGUpID0+IHtcbiAgICAgICAgICBjb25zdCBwcm9jZXNzZWRGaWxlVHlwZSA9IE9iamVjdC5wcm90b3R5cGUudG9TdHJpbmcuY2FsbChwcm9jZXNzZWRGaWxlKTtcbiAgICAgICAgICBpZiAocHJvY2Vzc2VkRmlsZVR5cGUgPT09ICdbb2JqZWN0IEZpbGVdJyB8fCBwcm9jZXNzZWRGaWxlVHlwZSA9PT0gJ1tvYmplY3QgQmxvYl0nKSB7XG4gICAgICAgICAgICB0aGlzLmF0dGFjaFVpZChwcm9jZXNzZWRGaWxlKTtcbiAgICAgICAgICAgIHRoaXMucG9zdChwcm9jZXNzZWRGaWxlKTtcbiAgICAgICAgICB9IGVsc2UgaWYgKHR5cGVvZiBwcm9jZXNzZWRGaWxlID09PSAnYm9vbGVhbicgJiYgcHJvY2Vzc2VkRmlsZSAhPT0gZmFsc2UpIHtcbiAgICAgICAgICAgIHRoaXMucG9zdChmaWxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0sXG4gICAgICAgIGUgPT4ge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgVW5oYW5kbGVkIHVwbG9hZCBiZWZvcmVVcGxvYWQgZXJyb3JgLCBlKTtcbiAgICAgICAgfVxuICAgICAgKTtcbiAgICB9IGVsc2UgaWYgKGJlZm9yZSAhPT0gZmFsc2UpIHtcbiAgICAgIHJldHVybiB0aGlzLnBvc3QoZmlsZSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBwb3N0KGZpbGU6IFVwbG9hZEZpbGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5kZXN0cm95KSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNvbnN0IG9wdCA9IHRoaXMub3B0aW9ucztcbiAgICBjb25zdCB7IHVpZCB9ID0gZmlsZTtcbiAgICBsZXQgeyBkYXRhLCBoZWFkZXJzIH0gPSBvcHQ7XG4gICAgaWYgKHR5cGVvZiBkYXRhID09PSAnZnVuY3Rpb24nKSB7XG4gICAgICBkYXRhID0gKGRhdGEgYXMgKGZpbGU6IFVwbG9hZEZpbGUpID0+IHt9KShmaWxlKTtcbiAgICB9XG4gICAgaWYgKHR5cGVvZiBoZWFkZXJzID09PSAnZnVuY3Rpb24nKSB7XG4gICAgICBoZWFkZXJzID0gKGhlYWRlcnMgYXMgKGZpbGU6IFVwbG9hZEZpbGUpID0+IHt9KShmaWxlKTtcbiAgICB9XG4gICAgY29uc3QgYXJnczogVXBsb2FkWEhSQXJncyA9IHtcbiAgICAgIGFjdGlvbjogb3B0LmFjdGlvbixcbiAgICAgIG5hbWU6IG9wdC5uYW1lLFxuICAgICAgaGVhZGVycyxcbiAgICAgIGZpbGUsXG4gICAgICBkYXRhLFxuICAgICAgd2l0aENyZWRlbnRpYWxzOiBvcHQud2l0aENyZWRlbnRpYWxzLFxuICAgICAgb25Qcm9ncmVzczogb3B0Lm9uUHJvZ3Jlc3NcbiAgICAgICAgPyBlID0+IHtcbiAgICAgICAgICAgIG9wdC5vblByb2dyZXNzIShlLCBmaWxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIDogdW5kZWZpbmVkLFxuICAgICAgb25TdWNjZXNzOiAocmV0LCB4aHIpID0+IHtcbiAgICAgICAgdGhpcy5jbGVhbih1aWQpO1xuICAgICAgICBvcHQub25TdWNjZXNzIShyZXQsIGZpbGUsIHhocik7XG4gICAgICB9LFxuICAgICAgb25FcnJvcjogeGhyID0+IHtcbiAgICAgICAgdGhpcy5jbGVhbih1aWQpO1xuICAgICAgICBvcHQub25FcnJvciEoeGhyLCBmaWxlKTtcbiAgICAgIH1cbiAgICB9O1xuICAgIGNvbnN0IHJlcSQgPSAob3B0LmN1c3RvbVJlcXVlc3QgfHwgdGhpcy54aHIpLmNhbGwodGhpcywgYXJncyk7XG4gICAgaWYgKCEocmVxJCBpbnN0YW5jZW9mIFN1YnNjcmlwdGlvbikpIHtcbiAgICAgIGNvbnNvbGUud2FybihgTXVzdCByZXR1cm4gU3Vic2NyaXB0aW9uIHR5cGUgaW4gJ1tuekN1c3RvbVJlcXVlc3RdJyBwcm9wZXJ0eWApO1xuICAgIH1cbiAgICB0aGlzLnJlcXNbdWlkXSA9IHJlcSQ7XG4gICAgb3B0Lm9uU3RhcnQhKGZpbGUpO1xuICB9XG5cbiAgcHJpdmF0ZSB4aHIoYXJnczogVXBsb2FkWEhSQXJncyk6IFN1YnNjcmlwdGlvbiB7XG4gICAgY29uc3QgZm9ybURhdGEgPSBuZXcgRm9ybURhdGEoKTtcbiAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgZm9ybURhdGEuYXBwZW5kKGFyZ3MubmFtZSEsIGFyZ3MuZmlsZSBhcyBhbnkpO1xuICAgIGlmIChhcmdzLmRhdGEpIHtcbiAgICAgIE9iamVjdC5rZXlzKGFyZ3MuZGF0YSkubWFwKGtleSA9PiB7XG4gICAgICAgIGZvcm1EYXRhLmFwcGVuZChrZXksIGFyZ3MuZGF0YSFba2V5XSk7XG4gICAgICB9KTtcbiAgICB9XG4gICAgaWYgKCFhcmdzLmhlYWRlcnMpIHtcbiAgICAgIGFyZ3MuaGVhZGVycyA9IHt9O1xuICAgIH1cbiAgICBpZiAoYXJncy5oZWFkZXJzWydYLVJlcXVlc3RlZC1XaXRoJ10gIT09IG51bGwpIHtcbiAgICAgIGFyZ3MuaGVhZGVyc1snWC1SZXF1ZXN0ZWQtV2l0aCddID0gYFhNTEh0dHBSZXF1ZXN0YDtcbiAgICB9IGVsc2Uge1xuICAgICAgZGVsZXRlIGFyZ3MuaGVhZGVyc1snWC1SZXF1ZXN0ZWQtV2l0aCddO1xuICAgIH1cbiAgICBjb25zdCByZXEgPSBuZXcgSHR0cFJlcXVlc3QoJ1BPU1QnLCBhcmdzLmFjdGlvbiEsIGZvcm1EYXRhLCB7XG4gICAgICByZXBvcnRQcm9ncmVzczogdHJ1ZSxcbiAgICAgIHdpdGhDcmVkZW50aWFsczogYXJncy53aXRoQ3JlZGVudGlhbHMsXG4gICAgICBoZWFkZXJzOiBuZXcgSHR0cEhlYWRlcnMoYXJncy5oZWFkZXJzKVxuICAgIH0pO1xuICAgIHJldHVybiB0aGlzLmh0dHAucmVxdWVzdChyZXEpLnN1YnNjcmliZShcbiAgICAgIChldmVudDogSHR0cEV2ZW50PHt9PikgPT4ge1xuICAgICAgICBpZiAoZXZlbnQudHlwZSA9PT0gSHR0cEV2ZW50VHlwZS5VcGxvYWRQcm9ncmVzcykge1xuICAgICAgICAgIGlmIChldmVudC50b3RhbCEgPiAwKSB7XG4gICAgICAgICAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgICAgICAgICAoZXZlbnQgYXMgYW55KS5wZXJjZW50ID0gKGV2ZW50LmxvYWRlZCAvIGV2ZW50LnRvdGFsISkgKiAxMDA7XG4gICAgICAgICAgfVxuICAgICAgICAgIGFyZ3Mub25Qcm9ncmVzcyEoZXZlbnQsIGFyZ3MuZmlsZSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZXZlbnQgaW5zdGFuY2VvZiBIdHRwUmVzcG9uc2UpIHtcbiAgICAgICAgICBhcmdzLm9uU3VjY2VzcyEoZXZlbnQuYm9keSwgYXJncy5maWxlLCBldmVudCk7XG4gICAgICAgIH1cbiAgICAgIH0sXG4gICAgICBlcnIgPT4ge1xuICAgICAgICB0aGlzLmFib3J0KGFyZ3MuZmlsZSk7XG4gICAgICAgIGFyZ3Mub25FcnJvciEoZXJyLCBhcmdzLmZpbGUpO1xuICAgICAgfVxuICAgICk7XG4gIH1cblxuICBwcml2YXRlIGNsZWFuKHVpZDogc3RyaW5nKTogdm9pZCB7XG4gICAgY29uc3QgcmVxJCA9IHRoaXMucmVxc1t1aWRdO1xuICAgIGlmIChyZXEkIGluc3RhbmNlb2YgU3Vic2NyaXB0aW9uKSB7XG4gICAgICByZXEkLnVuc3Vic2NyaWJlKCk7XG4gICAgfVxuICAgIGRlbGV0ZSB0aGlzLnJlcXNbdWlkXTtcbiAgfVxuXG4gIGFib3J0KGZpbGU/OiBVcGxvYWRGaWxlKTogdm9pZCB7XG4gICAgaWYgKGZpbGUpIHtcbiAgICAgIHRoaXMuY2xlYW4oZmlsZSAmJiBmaWxlLnVpZCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIE9iamVjdC5rZXlzKHRoaXMucmVxcykuZm9yRWFjaCh1aWQgPT4gdGhpcy5jbGVhbih1aWQpKTtcbiAgICB9XG4gIH1cblxuICAvLyAjcmVnaW9uIHN0eWxlc1xuXG4gIHByaXZhdGUgcHJlZml4Q2xzID0gJ2FudC11cGxvYWQnO1xuXG4gIHByaXZhdGUgc2V0Q2xhc3NNYXAoKTogdm9pZCB7XG4gICAgY29uc3QgY2xhc3NNYXAgPSB7XG4gICAgICBbdGhpcy5wcmVmaXhDbHNdOiB0cnVlLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1kaXNhYmxlZGBdOiB0aGlzLm9wdGlvbnMuZGlzYWJsZWQsXG4gICAgICAuLi50aGlzLmNsYXNzZXNcbiAgICB9O1xuICAgIHRoaXMudXBkYXRlSG9zdENsYXNzU2VydmljZS51cGRhdGVIb3N0Q2xhc3ModGhpcy5lbC5uYXRpdmVFbGVtZW50LCBjbGFzc01hcCk7XG4gIH1cblxuICAvLyAjZW5kcmVnaW9uXG5cbiAgY29uc3RydWN0b3IoXG4gICAgQE9wdGlvbmFsKCkgcHJpdmF0ZSBodHRwOiBIdHRwQ2xpZW50LFxuICAgIHByaXZhdGUgZWw6IEVsZW1lbnRSZWYsXG4gICAgcHJpdmF0ZSB1cGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2VcbiAgKSB7XG4gICAgaWYgKCFodHRwKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYE5vdCBmb3VuZCAnSHR0cENsaWVudCcsIFlvdSBjYW4gaW1wb3J0ICdIdHRwQ2xpZW50TW9kdWxlJyBpbiB5b3VyIHJvb3QgbW9kdWxlLmApO1xuICAgIH1cbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuaW5pdGVkID0gdHJ1ZTtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pbml0ZWQpIHtcbiAgICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgICB9XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3kgPSB0cnVlO1xuICAgIHRoaXMuYWJvcnQoKTtcbiAgfVxufVxuIl19