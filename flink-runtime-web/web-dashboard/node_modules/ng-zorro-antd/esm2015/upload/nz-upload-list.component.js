/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Input, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
export class NzUploadListComponent {
    // #endregion
    /**
     * @param {?} el
     * @param {?} cdr
     * @param {?} updateHostClassService
     */
    constructor(el, cdr, updateHostClassService) {
        this.el = el;
        this.cdr = cdr;
        this.updateHostClassService = updateHostClassService;
        this.imageTypes = ['image', 'webp', 'png', 'svg', 'gif', 'jpg', 'jpeg', 'bmp'];
        // #region fields
        // tslint:disable-next-line:no-any
        this.locale = {};
        // #endregion
        // #region styles
        this.prefixCls = 'ant-upload-list';
    }
    /**
     * @return {?}
     */
    get showPic() {
        return this.listType === 'picture' || this.listType === 'picture-card';
    }
    /**
     * @param {?} list
     * @return {?}
     */
    set items(list) {
        list.forEach((/**
         * @param {?} file
         * @return {?}
         */
        file => {
            file.linkProps = typeof file.linkProps === 'string' ? JSON.parse(file.linkProps) : file.linkProps;
        }));
        this._items = list;
    }
    /**
     * @return {?}
     */
    get items() {
        return this._items;
    }
    /**
     * @private
     * @return {?}
     */
    setClassMap() {
        /** @type {?} */
        const classMap = {
            [this.prefixCls]: true,
            [`${this.prefixCls}-${this.listType}`]: true
        };
        this.updateHostClassService.updateHostClass(this.el.nativeElement, classMap);
    }
    // #endregion
    // #region render
    /**
     * @private
     * @param {?} url
     * @return {?}
     */
    extname(url) {
        /** @type {?} */
        const temp = url.split('/');
        /** @type {?} */
        const filename = temp[temp.length - 1];
        /** @type {?} */
        const filenameWithoutSuffix = filename.split(/#|\?/)[0];
        return (/\.[^./\\]*$/.exec(filenameWithoutSuffix) || [''])[0];
    }
    /**
     * @param {?} file
     * @return {?}
     */
    isImageUrl(file) {
        if (~this.imageTypes.indexOf(file.type)) {
            return true;
        }
        /** @type {?} */
        const url = (/** @type {?} */ ((file.thumbUrl || file.url || '')));
        if (!url) {
            return false;
        }
        /** @type {?} */
        const extension = this.extname(url);
        if (/^data:image\//.test(url) || /(webp|svg|png|gif|jpg|jpeg|bmp)$/i.test(extension)) {
            return true;
        }
        else if (/^data:/.test(url)) {
            // other file types of base64
            return false;
        }
        else if (extension) {
            // other file types which have extension
            return false;
        }
        return true;
    }
    /**
     * @private
     * @param {?} file
     * @param {?} callback
     * @return {?}
     */
    previewFile(file, callback) {
        if (file.type && this.imageTypes.indexOf(file.type) === -1) {
            callback('');
        }
        /** @type {?} */
        const reader = new FileReader();
        // https://developer.mozilla.org/en-US/docs/Web/API/FileReader/readAsDataURL
        reader.onloadend = (/**
         * @return {?}
         */
        () => callback((/** @type {?} */ (reader.result))));
        reader.readAsDataURL(file);
    }
    /**
     * @private
     * @return {?}
     */
    genThumb() {
        // tslint:disable-next-line:no-any
        /** @type {?} */
        const win = (/** @type {?} */ (window));
        if (!this.showPic ||
            typeof document === 'undefined' ||
            typeof win === 'undefined' ||
            !win.FileReader ||
            !win.File) {
            return;
        }
        this.items
            .filter((/**
         * @param {?} file
         * @return {?}
         */
        file => file.originFileObj instanceof File && file.thumbUrl === undefined))
            .forEach((/**
         * @param {?} file
         * @return {?}
         */
        file => {
            file.thumbUrl = '';
            this.previewFile((/** @type {?} */ (file.originFileObj)), (/**
             * @param {?} previewDataUrl
             * @return {?}
             */
            (previewDataUrl) => {
                file.thumbUrl = previewDataUrl;
                this.detectChanges();
            }));
        }));
    }
    /**
     * @param {?} file
     * @return {?}
     */
    showPreview(file) {
        const { showPreviewIcon, hidePreviewIconInNonImage } = this.icons;
        if (!showPreviewIcon) {
            return false;
        }
        return this.isImageUrl(file) ? true : !hidePreviewIconInNonImage;
    }
    /**
     * @param {?} file
     * @param {?} e
     * @return {?}
     */
    handlePreview(file, e) {
        if (!this.onPreview) {
            return;
        }
        e.preventDefault();
        return this.onPreview(file);
    }
    /**
     * @param {?} file
     * @param {?} e
     * @return {?}
     */
    handleRemove(file, e) {
        e.preventDefault();
        if (this.onRemove) {
            this.onRemove(file);
        }
        return;
    }
    /**
     * @return {?}
     */
    detectChanges() {
        this.cdr.detectChanges();
    }
    /**
     * @return {?}
     */
    ngOnChanges() {
        this.setClassMap();
        this.genThumb();
    }
}
NzUploadListComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-upload-list',
                template: "<div *ngFor=\"let file of items\" class=\"ant-upload-list-item ant-upload-list-item-{{file.status}}\" @itemState>\n  <ng-template #icon>\n    <ng-container *ngIf=\"showPic; else noPicTpl\">\n      <div *ngIf=\"listType === 'picture-card' && file.status === 'uploading'; else thumbUrlCheck\" class=\"ant-upload-list-item-uploading-text\">{{ locale.uploading }}</div>\n    </ng-container>\n    <ng-template #thumbUrlCheck>\n      <i *ngIf=\"!file.thumbUrl && !file.url; else thumbTpl\"\n        class=\"ant-upload-list-item-thumbnail\" nz-icon type=\"picture\" theme=\"twotone\"></i>\n    </ng-template>\n    <ng-template #thumbTpl>\n      <a class=\"ant-upload-list-item-thumbnail\" target=\"_blank\" rel=\"noopener noreferrer\"\n        [href]=\"file.thumbUrl || file.url\"\n        (click)=\"handlePreview(file, $event)\">\n        <img *ngIf=\"isImageUrl(file); else noThumbTpl\" [src]=\"file.thumbUrl || file.url\" [attr.alt]=\"file.name\" />\n      </a>\n    </ng-template>\n    <ng-template #noThumbTpl><i class=\"ant-upload-list-item-icon\" nz-icon type=\"file\" theme=\"twotone\"></i></ng-template>\n    <ng-template #noPicTpl><i nz-icon [type]=\"file.status === 'uploading' ? 'loading' : 'paper-clip'\"></i></ng-template>\n  </ng-template>\n  <ng-template #preview>\n    <ng-container *ngIf=\"file.url; else prevText\">\n      <a [href]=\"file.thumbUrl || file.url\" target=\"_blank\" rel=\"noopener noreferrer\" [attr.download]=\"file.linkProps && file.linkProps.download\"\n        (click)=\"handlePreview(file, $event)\" class=\"ant-upload-list-item-name\" title=\"{{ file.name }}\">{{ file.name }}</a>\n    </ng-container>\n    <ng-template #prevText>\n      <span (click)=\"handlePreview(file, $event)\" class=\"ant-upload-list-item-name\" title=\"{{ file.name }}\">{{ file.name }}</span>\n    </ng-template>\n  </ng-template>\n  <div class=\"ant-upload-list-item-info\">\n    <span *ngIf=\"file.status === 'error'\" nz-tooltip [nzTitle]=\"file.message\">\n      <ng-template [ngTemplateOutlet]=\"icon\"></ng-template>\n      <ng-template [ngTemplateOutlet]=\"preview\"></ng-template>\n    </span>\n    <span *ngIf=\"file.status !== 'error'\">\n      <ng-template [ngTemplateOutlet]=\"icon\"></ng-template>\n      <ng-template [ngTemplateOutlet]=\"preview\"></ng-template>\n    </span>\n  </div>\n  <ng-container *ngIf=\"listType === 'picture-card' && file.status !== 'uploading'; else close\">\n    <span class=\"ant-upload-list-item-actions\">\n      <a *ngIf=\"showPreview(file)\" [href]=\"file.thumbUrl || file.url\"\n        target=\"_blank\" rel=\"noopener noreferrer\"\n        title=\"{{ locale.previewFile }}\"\n        [ngStyle]=\"!(file.url || file.thumbUrl) && {'opacity': .5, 'pointer-events': 'none'}\"\n        (click)=\"handlePreview(file, $event)\">\n          <i nz-icon type=\"eye-o\"></i>\n      </a>\n      <i *ngIf=\"icons.showRemoveIcon\" (click)=\"handleRemove(file, $event)\" class=\"anticon anticon-delete\" title=\"{{ locale.removeFile }}\"></i>\n    </span>\n  </ng-container>\n  <ng-template #close>\n    <i *ngIf=\"icons.showRemoveIcon\" (click)=\"handleRemove(file, $event)\" nz-icon type=\"close\" title=\"{{ locale.removeFile }}\"></i>\n  </ng-template>\n  <div *ngIf=\"file.status === 'uploading'\" class=\"ant-upload-list-item-progress\">\n    <nz-progress [nzPercent]=\"file.percent\" [nzShowInfo]=\"false\" [nzStrokeWidth]=\"2\"></nz-progress>\n  </div>\n</div>",
                providers: [NzUpdateHostClassService],
                animations: [
                    trigger('itemState', [
                        transition(':enter', [
                            style({ height: '0', width: '0', opacity: 0 }),
                            animate(150, style({ height: '*', width: '*', opacity: 1 }))
                        ]),
                        transition(':leave', [animate(150, style({ height: '0', width: '0', opacity: 0 }))])
                    ])
                ],
                preserveWhitespaces: false,
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush
            }] }
];
/** @nocollapse */
NzUploadListComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: ChangeDetectorRef },
    { type: NzUpdateHostClassService }
];
NzUploadListComponent.propDecorators = {
    locale: [{ type: Input }],
    listType: [{ type: Input }],
    items: [{ type: Input }],
    icons: [{ type: Input }],
    onPreview: [{ type: Input }],
    onRemove: [{ type: Input }]
};
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzUploadListComponent.prototype.imageTypes;
    /**
     * @type {?}
     * @private
     */
    NzUploadListComponent.prototype._items;
    /** @type {?} */
    NzUploadListComponent.prototype.locale;
    /** @type {?} */
    NzUploadListComponent.prototype.listType;
    /** @type {?} */
    NzUploadListComponent.prototype.icons;
    /** @type {?} */
    NzUploadListComponent.prototype.onPreview;
    /** @type {?} */
    NzUploadListComponent.prototype.onRemove;
    /**
     * @type {?}
     * @private
     */
    NzUploadListComponent.prototype.prefixCls;
    /**
     * @type {?}
     * @private
     */
    NzUploadListComponent.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzUploadListComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzUploadListComponent.prototype.updateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdXBsb2FkLWxpc3QuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInVwbG9hZC9uei11cGxvYWQtbGlzdC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxPQUFPLEVBQUUsS0FBSyxFQUFFLFVBQVUsRUFBRSxPQUFPLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQUMxRSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsVUFBVSxFQUNWLEtBQUssRUFFTCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLHdCQUF3QixFQUFFLE1BQU0sNENBQTRDLENBQUM7QUFxQnRGLE1BQU0sT0FBTyxxQkFBcUI7Ozs7Ozs7SUFxSWhDLFlBQ1UsRUFBYyxFQUNkLEdBQXNCLEVBQ3RCLHNCQUFnRDtRQUZoRCxPQUFFLEdBQUYsRUFBRSxDQUFZO1FBQ2QsUUFBRyxHQUFILEdBQUcsQ0FBbUI7UUFDdEIsMkJBQXNCLEdBQXRCLHNCQUFzQixDQUEwQjtRQXZJbEQsZUFBVSxHQUFHLENBQUMsT0FBTyxFQUFFLE1BQU0sRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFFLEtBQUssQ0FBQyxDQUFDOzs7UUFVekUsV0FBTSxHQUFRLEVBQUUsQ0FBQzs7O1FBb0JsQixjQUFTLEdBQUcsaUJBQWlCLENBQUM7SUEwR25DLENBQUM7Ozs7SUFySUosSUFBSSxPQUFPO1FBQ1QsT0FBTyxJQUFJLENBQUMsUUFBUSxLQUFLLFNBQVMsSUFBSSxJQUFJLENBQUMsUUFBUSxLQUFLLGNBQWMsQ0FBQztJQUN6RSxDQUFDOzs7OztJQU9ELElBQ0ksS0FBSyxDQUFDLElBQWtCO1FBQzFCLElBQUksQ0FBQyxPQUFPOzs7O1FBQUMsSUFBSSxDQUFDLEVBQUU7WUFDbEIsSUFBSSxDQUFD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QixFQUFFLEtBQUs7Z0JBQzFCLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTthQUNoRDs7OztZQTFCQyxVQUFVO1lBRlYsaUJBQWlCO1lBUVYsd0JBQXdCOzs7cUJBZ0M5QixLQUFLO3VCQUNMLEtBQUs7b0JBQ0wsS0FBSztvQkFVTCxLQUFLO3dCQUNMLEtBQUs7dUJBQ0wsS0FBSzs7Ozs7OztJQXhCTiwyQ0FBa0Y7Ozs7O0lBQ2xGLHVDQUE2Qjs7SUFTN0IsdUNBQTBCOztJQUMxQix5Q0FBa0M7O0lBV2xDLHNDQUF3Qzs7SUFDeEMsMENBQStDOztJQUMvQyx5Q0FBOEM7Ozs7O0lBTTlDLDBDQUFzQzs7Ozs7SUF1R3BDLG1DQUFzQjs7Ozs7SUFDdEIsb0NBQThCOzs7OztJQUM5Qix1REFBd0QiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBhbmltYXRlLCBzdHlsZSwgdHJhbnNpdGlvbiwgdHJpZ2dlciB9IGZyb20gJ0Bhbmd1bGFyL2FuaW1hdGlvbnMnO1xuaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuXG5pbXBvcnQgeyBTaG93VXBsb2FkTGlzdEludGVyZmFjZSwgVXBsb2FkRmlsZSwgVXBsb2FkTGlzdFR5cGUgfSBmcm9tICcuL2ludGVyZmFjZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LXVwbG9hZC1saXN0JyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LXVwbG9hZC1saXN0LmNvbXBvbmVudC5odG1sJyxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgYW5pbWF0aW9uczogW1xuICAgIHRyaWdnZXIoJ2l0ZW1TdGF0ZScsIFtcbiAgICAgIHRyYW5zaXRpb24oJzplbnRlcicsIFtcbiAgICAgICAgc3R5bGUoeyBoZWlnaHQ6ICcwJywgd2lkdGg6ICcwJywgb3BhY2l0eTogMCB9KSxcbiAgICAgICAgYW5pbWF0ZSgxNTAsIHN0eWxlKHsgaGVpZ2h0OiAnKicsIHdpZHRoOiAnKicsIG9wYWNpdHk6IDEgfSkpXG4gICAgICBdKSxcbiAgICAgIHRyYW5zaXRpb24oJzpsZWF2ZScsIFthbmltYXRlKDE1MCwgc3R5bGUoeyBoZWlnaHQ6ICcwJywgd2lkdGg6ICcwJywgb3BhY2l0eTogMCB9KSldKVxuICAgIF0pXG4gIF0sXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaFxufSlcbmV4cG9ydCBjbGFzcyBOelVwbG9hZExpc3RDb21wb25lbnQgaW1wbGVtZW50cyBPbkNoYW5nZXMge1xuICBwcml2YXRlIGltYWdlVHlwZXMgPSBbJ2ltYWdlJywgJ3dlYnAnLCAncG5nJywgJ3N2ZycsICdnaWYnLCAnanBnJywgJ2pwZWcnLCAnYm1wJ107XG4gIHByaXZhdGUgX2l0ZW1zOiBVcGxvYWRGaWxlW107XG5cbiAgZ2V0IHNob3dQaWMoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMubGlzdFR5cGUgPT09ICdwaWN0dXJlJyB8fCB0aGlzLmxpc3RUeXBlID09PSAncGljdHVyZS1jYXJkJztcbiAgfVxuXG4gIC8vICNyZWdpb24gZmllbGRzXG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBASW5wdXQoKSBsb2NhbGU6IGFueSA9IHt9O1xuICBASW5wdXQoKSBsaXN0VHlwZTogVXBsb2FkTGlzdFR5cGU7XG4gIEBJbnB1dCgpXG4gIHNldCBpdGVtcyhsaXN0OiBVcGxvYWRGaWxlW10pIHtcbiAgICBsaXN0LmZvckVhY2goZmlsZSA9PiB7XG4gICAgICBmaWxlLmxpbmtQcm9wcyA9IHR5cGVvZiBmaWxlLmxpbmtQcm9wcyA9PT0gJ3N0cmluZycgPyBKU09OLnBhcnNlKGZpbGUubGlua1Byb3BzKSA6IGZpbGUubGlua1Byb3BzO1xuICAgIH0pO1xuICAgIHRoaXMuX2l0ZW1zID0gbGlzdDtcbiAgfVxuICBnZXQgaXRlbXMoKTogVXBsb2FkRmlsZVtdIHtcbiAgICByZXR1cm4gdGhpcy5faXRlbXM7XG4gIH1cbiAgQElucHV0KCkgaWNvbnM6IFNob3dVcGxvYWRMaXN0SW50ZXJmYWNlO1xuICBASW5wdXQoKSBvblByZXZpZXc6IChmaWxlOiBVcGxvYWRGaWxlKSA9PiB2b2lkO1xuICBASW5wdXQoKSBvblJlbW92ZTogKGZpbGU6IFVwbG9hZEZpbGUpID0+IHZvaWQ7XG5cbiAgLy8gI2VuZHJlZ2lvblxuXG4gIC8vICNyZWdpb24gc3R5bGVzXG5cbiAgcHJpdmF0ZSBwcmVmaXhDbHMgPSAnYW50LXVwbG9hZC1saXN0JztcblxuICBwcml2YXRlIHNldENsYXNzTWFwKCk6IHZvaWQge1xuICAgIGNvbnN0IGNsYXNzTWFwID0ge1xuICAgICAgW3RoaXMucHJlZml4Q2xzXTogdHJ1ZSxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tJHt0aGlzLmxpc3RUeXBlfWBdOiB0cnVlXG4gICAgfTtcbiAgICB0aGlzLnVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWwubmF0aXZlRWxlbWVudCwgY2xhc3NNYXApO1xuICB9XG5cbiAgLy8gI2VuZHJlZ2lvblxuXG4gIC8vICNyZWdpb24gcmVuZGVyXG5cbiAgcHJpdmF0ZSBleHRuYW1lKHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgICBjb25zdCB0ZW1wID0gdXJsLnNwbGl0KCcvJyk7XG4gICAgY29uc3QgZmlsZW5hbWUgPSB0ZW1wW3RlbXAubGVuZ3RoIC0gMV07XG4gICAgY29uc3QgZmlsZW5hbWVXaXRob3V0U3VmZml4ID0gZmlsZW5hbWUuc3BsaXQoLyN8XFw/LylbMF07XG4gICAgcmV0dXJuICgvXFwuW14uL1xcXFxdKiQvLmV4ZWMoZmlsZW5hbWVXaXRob3V0U3VmZml4KSB8fCBbJyddKVswXTtcbiAgfVxuXG4gIGlzSW1hZ2VVcmwoZmlsZTogVXBsb2FkRmlsZSk6IGJvb2xlYW4ge1xuICAgIGlmICh+dGhpcy5pbWFnZVR5cGVzLmluZGV4T2YoZmlsZS50eXBlKSkge1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuICAgIGNvbnN0IHVybDogc3RyaW5nID0gKGZpbGUudGh1bWJVcmwgfHwgZmlsZS51cmwgfHwgJycpIGFzIHN0cmluZztcbiAgICBpZiAoIXVybCkge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICBjb25zdCBleHRlbnNpb24gPSB0aGlzLmV4dG5hbWUodXJsKTtcbiAgICBpZiAoL15kYXRhOmltYWdlXFwvLy50ZXN0KHVybCkgfHwgLyh3ZWJwfHN2Z3xwbmd8Z2lmfGpwZ3xqcGVnfGJtcCkkL2kudGVzdChleHRlbnNpb24pKSB7XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICB9IGVsc2UgaWYgKC9eZGF0YTovLnRlc3QodXJsKSkge1xuICAgICAgLy8gb3RoZXIgZmlsZSB0eXBlcyBvZiBiYXNlNjRcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9IGVsc2UgaWYgKGV4dGVuc2lvbikge1xuICAgICAgLy8gb3RoZXIgZmlsZSB0eXBlcyB3aGljaCBoYXZlIGV4dGVuc2lvblxuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHByaXZhdGUgcHJldmlld0ZpbGUoZmlsZTogRmlsZSB8IEJsb2IsIGNhbGxiYWNrOiAoZGF0YVVybDogc3RyaW5nKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgaWYgKGZpbGUudHlwZSAmJiB0aGlzLmltYWdlVHlwZXMuaW5kZXhPZihmaWxlLnR5cGUpID09PSAtMSkge1xuICAgICAgY2FsbGJhY2soJycpO1xuICAgIH1cbiAgICBjb25zdCByZWFkZXIgPSBuZXcgRmlsZVJlYWRlcigpO1xuICAgIC8vIGh0dHBzOi8vZGV2ZWxvcGVyLm1vemlsbGEub3JnL2VuLVVTL2RvY3MvV2ViL0FQSS9GaWxlUmVhZGVyL3JlYWRBc0RhdGFVUkxcbiAgICByZWFkZXIub25sb2FkZW5kID0gKCkgPT4gY2FsbGJhY2socmVhZGVyLnJlc3VsdCBhcyBzdHJpbmcpO1xuICAgIHJlYWRlci5yZWFkQXNEYXRhVVJMKGZpbGUpO1xuICB9XG5cbiAgcHJpdmF0ZSBnZW5UaHVtYigpOiB2b2lkIHtcbiAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgY29uc3Qgd2luID0gd2luZG93IGFzIGFueTtcbiAgICBpZiAoXG4gICAgICAhdGhpcy5zaG93UGljIHx8XG4gICAgICB0eXBlb2YgZG9jdW1lbnQgPT09ICd1bmRlZmluZWQnIHx8XG4gICAgICB0eXBlb2Ygd2luID09PSAndW5kZWZpbmVkJyB8fFxuICAgICAgIXdpbi5GaWxlUmVhZGVyIHx8XG4gICAgICAhd2luLkZpbGVcbiAgICApIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5pdGVtc1xuICAgICAgLmZpbHRlcihmaWxlID0+IGZpbGUub3JpZ2luRmlsZU9iaiBpbnN0YW5jZW9mIEZpbGUgJiYgZmlsZS50aHVtYlVybCA9PT0gdW5kZWZpbmVkKVxuICAgICAgLmZvckVhY2goZmlsZSA9PiB7XG4gICAgICAgIGZpbGUudGh1bWJVcmwgPSAnJztcbiAgICAgICAgdGhpcy5wcmV2aWV3RmlsZShmaWxlLm9yaWdpbkZpbGVPYmohLCAocHJldmlld0RhdGFVcmw6IHN0cmluZykgPT4ge1xuICAgICAgICAgIGZpbGUudGh1bWJVcmwgPSBwcmV2aWV3RGF0YVVybDtcbiAgICAgICAgICB0aGlzLmRldGVjdENoYW5nZXMoKTtcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgfVxuXG4gIHNob3dQcmV2aWV3KGZpbGU6IFVwbG9hZEZpbGUpOiBib29sZWFuIHtcbiAgICBjb25zdCB7IHNob3dQcmV2aWV3SWNvbiwgaGlkZVByZXZpZXdJY29uSW5Ob25JbWFnZSB9ID0gdGhpcy5pY29ucztcbiAgICBpZiAoIXNob3dQcmV2aWV3SWNvbikge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5pc0ltYWdlVXJsKGZpbGUpID8gdHJ1ZSA6ICFoaWRlUHJldmlld0ljb25Jbk5vbkltYWdlO1xuICB9XG5cbiAgaGFuZGxlUHJldmlldyhmaWxlOiBVcGxvYWRGaWxlLCBlOiBFdmVudCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5vblByZXZpZXcpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgcmV0dXJuIHRoaXMub25QcmV2aWV3KGZpbGUpO1xuICB9XG5cbiAgaGFuZGxlUmVtb3ZlKGZpbGU6IFVwbG9hZEZpbGUsIGU6IEV2ZW50KTogdm9pZCB7XG4gICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIGlmICh0aGlzLm9uUmVtb3ZlKSB7XG4gICAgICB0aGlzLm9uUmVtb3ZlKGZpbGUpO1xuICAgIH1cbiAgICByZXR1cm47XG4gIH1cblxuICAvLyAjZW5kcmVnaW9uXG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHJpdmF0ZSBlbDogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgcHJpdmF0ZSB1cGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2VcbiAgKSB7fVxuXG4gIGRldGVjdENoYW5nZXMoKTogdm9pZCB7XG4gICAgdGhpcy5jZHIuZGV0ZWN0Q2hhbmdlcygpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIHRoaXMuZ2VuVGh1bWIoKTtcbiAgfVxufVxuIl19