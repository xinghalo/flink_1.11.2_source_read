/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, NgZone, Output, QueryList, TemplateRef, ViewChild, ViewChildren, ViewEncapsulation } from '@angular/core';
import { fromEvent, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NzOptionLiComponent } from './nz-option-li.component';
import { NzSelectService } from './nz-select.service';
var NzOptionContainerComponent = /** @class */ (function () {
    function NzOptionContainerComponent(nzSelectService, cdr, ngZone) {
        this.nzSelectService = nzSelectService;
        this.cdr = cdr;
        this.ngZone = ngZone;
        this.destroy$ = new Subject();
        this.nzScrollToBottom = new EventEmitter();
    }
    /**
     * @param {?} option
     * @return {?}
     */
    NzOptionContainerComponent.prototype.scrollIntoViewIfNeeded = /**
     * @param {?} option
     * @return {?}
     */
    function (option) {
        var _this = this;
        // delay after open
        setTimeout((/**
         * @return {?}
         */
        function () {
            if (_this.listOfNzOptionLiComponent && _this.listOfNzOptionLiComponent.length && option) {
                /** @type {?} */
                var targetOption = _this.listOfNzOptionLiComponent.find((/**
                 * @param {?} o
                 * @return {?}
                 */
                function (o) {
                    return _this.nzSelectService.compareWith(o.nzOption.nzValue, option.nzValue);
                }));
                /* tslint:disable:no-any */
                if (targetOption && targetOption.el && ((/** @type {?} */ (targetOption.el))).scrollIntoViewIfNeeded) {
                    ((/** @type {?} */ (targetOption.el))).scrollIntoViewIfNeeded(false);
                }
                /* tslint:enable:no-any */
            }
        }));
    };
    /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    NzOptionContainerComponent.prototype.trackLabel = /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    function (_index, option) {
        return option.nzLabel;
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    NzOptionContainerComponent.prototype.trackValue = 
    // tslint:disable-next-line:no-any
    /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    function (_index, option) {
        return option.nzValue;
    };
    /**
     * @return {?}
     */
    NzOptionContainerComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.nzSelectService.activatedOption$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} option
         * @return {?}
         */
        function (option) {
            _this.scrollIntoViewIfNeeded((/** @type {?} */ (option)));
        }));
        this.nzSelectService.check$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @return {?}
         */
        function () {
            _this.cdr.markForCheck();
        }));
        this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        function () {
            /** @type {?} */
            var ul = _this.dropdownUl.nativeElement;
            fromEvent(ul, 'scroll')
                .pipe(takeUntil(_this.destroy$))
                .subscribe((/**
             * @param {?} e
             * @return {?}
             */
            function (e) {
                e.preventDefault();
                e.stopPropagation();
                if (ul && ul.scrollHeight < ul.clientHeight + ul.scrollTop + 10) {
                    _this.ngZone.run((/**
                     * @return {?}
                     */
                    function () {
                        _this.nzScrollToBottom.emit();
                    }));
                }
            }));
        }));
    };
    /**
     * @return {?}
     */
    NzOptionContainerComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    NzOptionContainerComponent.decorators = [
        { type: Component, args: [{
                    selector: '[nz-option-container]',
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    preserveWhitespaces: false,
                    template: "<ul #dropdownUl\n  class=\"ant-select-dropdown-menu ant-select-dropdown-menu-root ant-select-dropdown-menu-vertical\"\n  role=\"menu\"\n  tabindex=\"0\">\n  <li *ngIf=\"nzSelectService.isShowNotFound\"\n    nz-select-unselectable\n    class=\"ant-select-dropdown-menu-item ant-select-dropdown-menu-item-disabled\">\n    <nz-embed-empty [nzComponentName]=\"'select'\" [specificContent]=\"nzNotFoundContent\"></nz-embed-empty>\n  </li>\n  <li nz-option-li\n    *ngIf=\"nzSelectService.addedTagOption\"\n    [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n    [nzOption]=\"nzSelectService.addedTagOption\">\n  </li>\n  <li nz-option-li\n    *ngFor=\"let option of nzSelectService.listOfNzOptionComponent | nzFilterOption : nzSelectService.searchValue : nzSelectService.filterOption : nzSelectService.serverSearch; trackBy: trackValue\"\n    [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n    [nzOption]=\"option\">\n  </li>\n  <li class=\"ant-select-dropdown-menu-item-group\"\n    *ngFor=\"let group of nzSelectService.listOfNzOptionGroupComponent | nzFilterGroupOption : nzSelectService.searchValue : nzSelectService.filterOption :nzSelectService.serverSearch; trackBy: trackLabel\">\n    <div class=\"ant-select-dropdown-menu-item-group-title\"\n      [attr.title]=\"group.isLabelString ? group.nzLabel : ''\">\n      <ng-container *nzStringTemplateOutlet=\"group.nzLabel\"> {{group.nzLabel}} </ng-container>\n    </div>\n    <ul class=\"ant-select-dropdown-menu-item-group-list\">\n      <li nz-option-li\n        *ngFor=\"let option of group.listOfNzOptionComponent | nzFilterOption : nzSelectService.searchValue : nzSelectService.filterOption :nzSelectService.serverSearch; trackBy: trackValue\"\n        [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n        [nzOption]=\"option\">\n      </li>\n    </ul>\n  </li>\n  <li nz-option-li\n    *ngFor=\"let option of nzSelectService.listOfTagOption | nzFilterOption : nzSelectService.searchValue : nzSelectService.filterOption : nzSelectService.serverSearch; trackBy: trackValue \"\n    [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n    [nzOption]=\"option\">\n  </li>\n</ul>\n"
                }] }
    ];
    /** @nocollapse */
    NzOptionContainerComponent.ctorParameters = function () { return [
        { type: NzSelectService },
        { type: ChangeDetectorRef },
        { type: NgZone }
    ]; };
    NzOptionContainerComponent.propDecorators = {
        listOfNzOptionLiComponent: [{ type: ViewChildren, args: [NzOptionLiComponent,] }],
        dropdownUl: [{ type: ViewChild, args: ['dropdownUl',] }],
        nzNotFoundContent: [{ type: Input }],
        nzMenuItemSelectedIcon: [{ type: Input }],
        nzScrollToBottom: [{ type: Output }]
    };
    return NzOptionContainerComponent;
}());
export { NzOptionContainerComponent };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzOptionContainerComponent.prototype.destroy$;
    /** @type {?} */
    NzOptionContainerComponent.prototype.listOfNzOptionLiComponent;
    /** @type {?} */
    NzOptionContainerComponent.prototype.dropdownUl;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzNotFoundContent;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzMenuItemSelectedIcon;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzScrollToBottom;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzSelectService;
    /**
     * @type {?}
     * @private
     */
    NzOptionContainerComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzOptionContainerComponent.prototype.ngZone;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotb3B0aW9uLWNvbnRhaW5lci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic2VsZWN0L256LW9wdGlvbi1jb250YWluZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsVUFBVSxFQUNWLFlBQVksRUFDWixLQUFLLEVBQ0wsTUFBTSxFQUdOLE1BQU0sRUFDTixTQUFTLEVBQ1QsV0FBVyxFQUNYLFNBQVMsRUFDVCxZQUFZLEVBQ1osaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxTQUFTLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzFDLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUUzQyxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUUvRCxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFFdEQ7SUF3Q0Usb0NBQW1CLGVBQWdDLEVBQVUsR0FBc0IsRUFBVSxNQUFjO1FBQXhGLG9CQUFlLEdBQWYsZUFBZSxDQUFpQjtRQUFVLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBQVUsV0FBTSxHQUFOLE1BQU0sQ0FBUTtRQWhDbkcsYUFBUSxHQUFHLElBQUksT0FBTyxFQUFFLENBQUM7UUFLZCxxQkFBZ0IsR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO0lBMkIrQyxDQUFDOzs7OztJQXpCL0csMkRBQXNCOzs7O0lBQXRCLFVBQXVCLE1BQXlCO1FBQWhELGlCQWNDO1FBYkMsbUJBQW1CO1FBQ25CLFVBQVU7OztRQUFDO1lBQ1QsSUFBSSxLQUFJLENBQUMseUJBQXlCLElBQUksS0FBSSxDQUFDLHlCQUF5QixDQUFDLE1BQU0sSUFBSSxNQUFNLEVBQUU7O29CQUMvRSxZQUFZ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Q0EwQkwsWUFBWSxTQUFDLG1CQUFtQjs2QkFDaEMsU0FBUyxTQUFDLFlBQVk7b0NBQ3RCLEtBQUs7eUNBQ0wsS0FBSzttQ0FDTCxNQUFNOztJQXdEVCxpQ0FBQztDQUFBLEFBckVELElBcUVDO1NBOURZLDBCQUEwQjs7Ozs7O0lBQ3JDLDhDQUFpQzs7SUFDakMsK0RBQTZGOztJQUM3RixnREFBZ0Q7O0lBQ2hELHVEQUFtQzs7SUFDbkMsNERBQW1EOztJQUNuRCxzREFBK0Q7O0lBMkJuRCxxREFBdUM7Ozs7O0lBQUUseUNBQThCOzs7OztJQUFFLDRDQUFzQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBOZ1pvbmUsXG4gIE9uRGVzdHJveSxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFF1ZXJ5TGlzdCxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0NoaWxkcmVuLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IGZyb21FdmVudCwgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpPcHRpb25Hcm91cENvbXBvbmVudCB9IGZyb20gJy4vbnotb3B0aW9uLWdyb3VwLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOek9wdGlvbkxpQ29tcG9uZW50IH0gZnJvbSAnLi9uei1vcHRpb24tbGkuY29tcG9uZW50JztcbmltcG9ydCB7IE56T3B0aW9uQ29tcG9uZW50IH0gZnJvbSAnLi9uei1vcHRpb24uY29tcG9uZW50JztcbmltcG9ydCB7IE56U2VsZWN0U2VydmljZSB9IGZyb20gJy4vbnotc2VsZWN0LnNlcnZpY2UnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICdbbnotb3B0aW9uLWNvbnRhaW5lcl0nLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1vcHRpb24tY29udGFpbmVyLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOek9wdGlvbkNvbnRhaW5lckNvbXBvbmVudCBpbXBsZW1lbnRzIE9uRGVzdHJveSwgT25Jbml0IHtcbiAgcHJpdmF0ZSBkZXN0cm95JCA9IG5ldyBTdWJqZWN0KCk7XG4gIEBWaWV3Q2hpbGRyZW4oTnpPcHRpb25MaUNvbXBvbmVudCkgbGlzdE9mTnpPcHRpb25MaUNvbXBvbmVudDogUXVlcnlMaXN0PE56T3B0aW9uTGlDb21wb25lbnQ+O1xuICBAVmlld0NoaWxkKCdkcm9wZG93blVsJykgZHJvcGRvd25VbDogRWxlbWVudFJlZjtcbiAgQElucHV0KCkgbnpOb3RGb3VuZENvbnRlbnQ6IHN0cmluZztcbiAgQElucHV0KCkgbnpNZW51SXRlbVNlbGVjdGVkSWNvbjogVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelNjcm9sbFRvQm90dG9tID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuXG4gIHNjcm9sbEludG9WaWV3SWZOZWVkZWQob3B0aW9uOiBOek9wdGlvbkNvbXBvbmVudCk6IHZvaWQge1xuICAgIC8vIGRlbGF5IGFmdGVyIG9wZW5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIGlmICh0aGlzLmxpc3RPZk56T3B0aW9uTGlDb21wb25lbnQgJiYgdGhpcy5saXN0T2ZOek9wdGlvbkxpQ29tcG9uZW50Lmxlbmd0aCAmJiBvcHRpb24pIHtcbiAgICAgICAgY29uc3QgdGFyZ2V0T3B0aW9uID0gdGhpcy5saXN0T2ZOek9wdGlvbkxpQ29tcG9uZW50LmZpbmQobyA9PlxuICAgICAgICAgIHRoaXMubnpTZWxlY3RTZXJ2aWNlLmNvbXBhcmVXaXRoKG8ubnpPcHRpb24ubnpWYWx1ZSwgb3B0aW9uLm56VmFsdWUpXG4gICAgICAgICk7XG4gICAgICAgIC8qIHRzbGludDpkaXNhYmxlOm5vLWFueSAqL1xuICAgICAgICBpZiAodGFyZ2V0T3B0aW9uICYmIHRhcmdldE9wdGlvbi5lbCAmJiAodGFyZ2V0T3B0aW9uLmVsIGFzIGFueSkuc2Nyb2xsSW50b1ZpZXdJZk5lZWRlZCkge1xuICAgICAgICAgICh0YXJnZXRPcHRpb24uZWwgYXMgYW55KS5zY3JvbGxJbnRvVmlld0lmTmVlZGVkKGZhbHNlKTtcbiAgICAgICAgfVxuICAgICAgICAvKiB0c2xpbnQ6ZW5hYmxlOm5vLWFueSAqL1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgdHJhY2tMYWJlbChfaW5kZXg6IG51bWJlciwgb3B0aW9uOiBOek9wdGlvbkdyb3VwQ29tcG9uZW50KTogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD4ge1xuICAgIHJldHVybiBvcHRpb24ubnpMYWJlbDtcbiAgfVxuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgdHJhY2tWYWx1ZShfaW5kZXg6IG51bWJlciwgb3B0aW9uOiBOek9wdGlvbkNvbXBvbmVudCk6IGFueSB7XG4gICAgcmV0dXJuIG9wdGlvbi5uelZhbHVlO1xuICB9XG5cbiAgY29uc3RydWN0b3IocHVibGljIG56U2VsZWN0U2VydmljZTogTnpTZWxlY3RTZXJ2aWNlLCBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsIHByaXZhdGUgbmdab25lOiBOZ1pvbmUpIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2UuYWN0aXZhdGVkT3B0aW9uJC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSkuc3Vic2NyaWJlKG9wdGlvbiA9PiB7XG4gICAgICB0aGlzLnNjcm9sbEludG9WaWV3SWZOZWVkZWQob3B0aW9uISk7XG4gICAgfSk7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2UuY2hlY2skLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfSk7XG4gICAgdGhpcy5uZ1pvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgY29uc3QgdWwgPSB0aGlzLmRyb3Bkb3duVWwubmF0aXZlRWxlbWVudDtcbiAgICAgIGZyb21FdmVudDxNb3VzZUV2ZW50Pih1bCwgJ3Njcm9sbCcpXG4gICAgICAgIC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSlcbiAgICAgICAgLnN1YnNjcmliZShlID0+IHtcbiAgICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgZS5zdG9wUHJvcGFnYXRpb24oKTtcbiAgICAgICAgICBpZiAodWwgJiYgdWwuc2Nyb2xsSGVpZ2h0IDwgdWwuY2xpZW50SGVpZ2h0ICsgdWwuc2Nyb2xsVG9wICsgMTApIHtcbiAgICAgICAgICAgIHRoaXMubmdab25lLnJ1bigoKSA9PiB7XG4gICAgICAgICAgICAgIHRoaXMubnpTY3JvbGxUb0JvdHRvbS5lbWl0KCk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgIH0pO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG59XG4iXX0=