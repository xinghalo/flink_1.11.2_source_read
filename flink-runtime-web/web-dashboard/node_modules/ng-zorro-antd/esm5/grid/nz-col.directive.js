/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { Directive, ElementRef, Host, Input, Optional, Renderer2 } from '@angular/core';
import { Subject } from 'rxjs';
import { startWith, takeUntil } from 'rxjs/operators';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { isNotNil } from '../core/util/check';
import { NzRowDirective } from './nz-row.directive';
/**
 * @record
 */
export function EmbeddedProperty() { }
if (false) {
    /** @type {?} */
    EmbeddedProperty.prototype.span;
    /** @type {?} */
    EmbeddedProperty.prototype.pull;
    /** @type {?} */
    EmbeddedProperty.prototype.push;
    /** @type {?} */
    EmbeddedProperty.prototype.offset;
    /** @type {?} */
    EmbeddedProperty.prototype.order;
}
var NzColDirective = /** @class */ (function () {
    function NzColDirective(nzUpdateHostClassService, elementRef, nzRowDirective, renderer) {
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.elementRef = elementRef;
        this.nzRowDirective = nzRowDirective;
        this.renderer = renderer;
        this.el = this.elementRef.nativeElement;
        this.prefixCls = 'ant-col';
        this.destroy$ = new Subject();
    }
    /** temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289*/
    // tslint:disable-line:no-any
    /**
     * temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289
     * @return {?}
     */
    NzColDirective.prototype.setClassMap = 
    // tslint:disable-line:no-any
    /**
     * temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var classMap = tslib_1.__assign((_a = {}, _a[this.prefixCls + "-" + this.nzSpan] = isNotNil(this.nzSpan), _a[this.prefixCls + "-order-" + this.nzOrder] = isNotNil(this.nzOrder), _a[this.prefixCls + "-offset-" + this.nzOffset] = isNotNil(this.nzOffset), _a[this.prefixCls + "-pull-" + this.nzPull] = isNotNil(this.nzPull), _a[this.prefixCls + "-push-" + this.nzPush] = isNotNil(this.nzPush), _a), this.generateClass());
        this.nzUpdateHostClassService.updateHostClass(this.el, classMap);
    };
    /**
     * @return {?}
     */
    NzColDirective.prototype.generateClass = /**
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var listOfSizeInputName = ['nzXs', 'nzSm', 'nzMd', 'nzLg', 'nzXl', 'nzXXl'];
        /** @type {?} */
        var listClassMap = {};
        listOfSizeInputName.forEach((/**
         * @param {?} name
         * @return {?}
         */
        function (name) {
            /** @type {?} */
            var sizeName = name.replace('nz', '').toLowerCase();
            if (isNotNil(_this[name])) {
                if (typeof _this[name] === 'number' || typeof _this[name] === 'string') {
                    listClassMap[_this.prefixCls + "-" + sizeName + "-" + _this[name]] = true;
                }
                else {
                    listClassMap[_this.prefixCls + "-" + sizeName + "-" + _this[name].span] = _this[name] && isNotNil(_this[name].span);
                    listClassMap[_this.prefixCls + "-" + sizeName + "-pull-" + _this[name].pull] =
                        _this[name] && isNotNil(_this[name].pull);
                    listClassMap[_this.prefixCls + "-" + sizeName + "-push-" + _this[name].push] =
                        _this[name] && isNotNil(_this[name].push);
                    listClassMap[_this.prefixCls + "-" + sizeName + "-offset-" + _this[name].offset] =
                        _this[name] && isNotNil(_this[name].offset);
                    listClassMap[_this.prefixCls + "-" + sizeName + "-order-" + _this[name].order] =
                        _this[name] && isNotNil(_this[name].order);
                }
            }
        }));
        return listClassMap;
    };
    /**
     * @return {?}
     */
    NzColDirective.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        this.setClassMap();
    };
    /**
     * @return {?}
     */
    NzColDirective.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        if (this.nzRowDirective) {
            this.nzRowDirective.actualGutter$
                .pipe(startWith(this.nzRowDirective.actualGutter), takeUntil(this.destroy$))
                .subscribe((/**
             * @param {?} actualGutter
             * @return {?}
             */
            function (actualGutter) {
                _this.renderer.setStyle(_this.el, 'padding-left', actualGutter / 2 + "px");
                _this.renderer.setStyle(_this.el, 'padding-right', actualGutter / 2 + "px");
            }));
        }
    };
    /**
     * @return {?}
     */
    NzColDirective.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setClassMap();
    };
    /**
     * @return {?}
     */
    NzColDirective.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    NzColDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[nz-col],nz-col',
                    providers: [NzUpdateHostClassService]
                },] }
    ];
    /** @nocollapse */
    NzColDirective.ctorParameters = function () { return [
        { type: NzUpdateHostClassService },
        { type: ElementRef },
        { type: NzRowDirective, decorators: [{ type: Optional }, { type: Host }] },
        { type: Renderer2 }
    ]; };
    NzColDirective.propDecorators = {
        nzSpan: [{ type: Input }],
        nzOrder: [{ type: Input }],
        nzOffset: [{ type: Input }],
        nzPush: [{ type: Input }],
        nzPull: [{ type: Input }],
        nzXs: [{ type: Input }],
        nzSm: [{ type: Input }],
        nzMd: [{ type: Input }],
        nzLg: [{ type: Input }],
        nzXl: [{ type: Input }],
        nzXXl: [{ type: Input }]
    };
    return NzColDirective;
}());
export { NzColDirective };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzColDirective.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzColDirective.prototype.prefixCls;
    /**
     * @type {?}
     * @protected
     */
    NzColDirective.prototype.destroy$;
    /** @type {?} */
    NzColDirective.prototype.nzSpan;
    /** @type {?} */
    NzColDirective.prototype.nzOrder;
    /** @type {?} */
    NzColDirective.prototype.nzOffset;
    /** @type {?} */
    NzColDirective.prototype.nzPush;
    /** @type {?} */
    NzColDirective.prototype.nzPull;
    /** @type {?} */
    NzColDirective.prototype.nzXs;
    /** @type {?} */
    NzColDirective.prototype.nzSm;
    /** @type {?} */
    NzColDirective.prototype.nzMd;
    /** @type {?} */
    NzColDirective.prototype.nzLg;
    /** @type {?} */
    NzColDirective.prototype.nzXl;
    /** @type {?} */
    NzColDirective.prototype.nzXXl;
    /**
     * @type {?}
     * @private
     */
    NzColDirective.prototype.nzUpdateHostClassService;
    /**
     * @type {?}
     * @private
     */
    NzColDirective.prototype.elementRef;
    /** @type {?} */
    NzColDirective.prototype.nzRowDirective;
    /** @type {?} */
    NzColDirective.prototype.renderer;
    /* Skipping unhandled member: [property: string]: any;*/
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY29sLmRpcmVjdGl2ZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJncmlkL256LWNvbC5kaXJlY3RpdmUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxPQUFPLEVBRUwsU0FBUyxFQUNULFVBQVUsRUFDVixJQUFJLEVBQ0osS0FBSyxFQUlMLFFBQVEsRUFDUixTQUFTLEVBQ1YsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUMvQixPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRXRELE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBRXRGLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUU5QyxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7Ozs7QUFFcEQsc0NBTUM7OztJQUxDLGdDQUFhOztJQUNiLGdDQUFhOztJQUNiLGdDQUFhOztJQUNiLGtDQUFlOztJQUNmLGlDQUFjOztBQUdoQjtJQTRERSx3QkFDVSx3QkFBa0QsRUFDbEQsVUFBc0IsRUFDSCxjQUE4QixFQUNsRCxRQUFtQjtRQUhsQiw2QkFBd0IsR0FBeEIsd0JBQXdCLENBQTBCO1FBQ2xELGVBQVUsR0FBVixVQUFVLENBQVk7UUFDSCxtQkFBYyxHQUFkLGNBQWMsQ0FBZ0I7UUFDbEQsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQTNEcEIsT0FBRSxHQUFnQixJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQztRQUNoRCxjQUFTLEdBQUcsU0FBUyxDQUFDO1FBQ3BCLGFBQVEsR0FBRyxJQUFJLE9BQU8sRUFBRSxDQUFDO0lBMERoQyxDQUFDO0lBMUNKLHVHQUF1Rzs7Ozs7O0lBQ3ZHLG9DQUFXOzs7Ozs7SUFBWDs7O1lBQ1EsUUFBUSxpQ0FDUixJQUFJLENBQUMsU0FB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Uzs7O3lCQTRCUixLQUFLOzBCQUNMLEtBQUs7MkJBQ0wsS0FBSzt5QkFDTCxLQUFLO3lCQUNMLEtBQUs7dUJBQ0wsS0FBSzt1QkFDTCxLQUFLO3VCQUNMLEtBQUs7dUJBQ0wsS0FBSzt1QkFDTCxLQUFLO3dCQUNMLEtBQUs7O0lBMEVSLHFCQUFDO0NBQUEsQUE3RkQsSUE2RkM7U0F6RlksY0FBYzs7Ozs7O0lBQ3pCLDRCQUF3RDs7Ozs7SUFDeEQsbUNBQThCOzs7OztJQUM5QixrQ0FBbUM7O0lBRW5DLGdDQUF3Qjs7SUFDeEIsaUNBQXlCOztJQUN6QixrQ0FBMEI7O0lBQzFCLGdDQUF3Qjs7SUFDeEIsZ0NBQXdCOztJQUN4Qiw4QkFBeUM7O0lBQ3pDLDhCQUF5Qzs7SUFDekMsOEJBQXlDOztJQUN6Qyw4QkFBeUM7O0lBQ3pDLDhCQUF5Qzs7SUFDekMsK0JBQTBDOzs7OztJQTBDeEMsa0RBQTBEOzs7OztJQUMxRCxvQ0FBOEI7O0lBQzlCLHdDQUF5RDs7SUFDekQsa0NBQTBCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgRGlyZWN0aXZlLFxuICBFbGVtZW50UmVmLFxuICBIb3N0LFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3B0aW9uYWwsXG4gIFJlbmRlcmVyMlxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IHN0YXJ0V2l0aCwgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuaW1wb3J0IHsgTmdDbGFzc0ludGVyZmFjZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvbmctY2xhc3MnO1xuaW1wb3J0IHsgaXNOb3ROaWwgfSBmcm9tICcuLi9jb3JlL3V0aWwvY2hlY2snO1xuXG5pbXBvcnQgeyBOelJvd0RpcmVjdGl2ZSB9IGZyb20gJy4vbnotcm93LmRpcmVjdGl2ZSc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgRW1iZWRkZWRQcm9wZXJ0eSB7XG4gIHNwYW46IG51bWJlcjtcbiAgcHVsbDogbnVtYmVyO1xuICBwdXNoOiBudW1iZXI7XG4gIG9mZnNldDogbnVtYmVyO1xuICBvcmRlcjogbnVtYmVyO1xufVxuXG5ARGlyZWN0aXZlKHtcbiAgc2VsZWN0b3I6ICdbbnotY29sXSxuei1jb2wnLFxuICBwcm92aWRlcnM6IFtOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2VdXG59KVxuZXhwb3J0IGNsYXNzIE56Q29sRGlyZWN0aXZlIGltcGxlbWVudHMgT25Jbml0LCBPbkNoYW5nZXMsIEFmdGVyVmlld0luaXQsIE9uRGVzdHJveSB7XG4gIHByaXZhdGUgZWw6IEhUTUxFbGVtZW50ID0gdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQ7XG4gIHByaXZhdGUgcHJlZml4Q2xzID0gJ2FudC1jb2wnO1xuICBwcm90ZWN0ZWQgZGVzdHJveSQgPSBuZXcgU3ViamVjdCgpO1xuXG4gIEBJbnB1dCgpIG56U3BhbjogbnVtYmVyO1xuICBASW5wdXQoKSBuek9yZGVyOiBudW1iZXI7XG4gIEBJbnB1dCgpIG56T2Zmc2V0OiBudW1iZXI7XG4gIEBJbnB1dCgpIG56UHVzaDogbnVtYmVyO1xuICBASW5wdXQoKSBuelB1bGw6IG51bWJlcjtcbiAgQElucHV0KCkgbnpYczogbnVtYmVyIHwgRW1iZWRkZWRQcm9wZXJ0eTtcbiAgQElucHV0KCkgbnpTbTogbnVtYmVyIHwgRW1iZWRkZWRQcm9wZXJ0eTtcbiAgQElucHV0KCkgbnpNZDogbnVtYmVyIHwgRW1iZWRkZWRQcm9wZXJ0eTtcbiAgQElucHV0KCkgbnpMZzogbnVtYmVyIHwgRW1iZWRkZWRQcm9wZXJ0eTtcbiAgQElucHV0KCkgbnpYbDogbnVtYmVyIHwgRW1iZWRkZWRQcm9wZXJ0eTtcbiAgQElucHV0KCkgbnpYWGw6IG51bWJlciB8IEVtYmVkZGVkUHJvcGVydHk7XG5cbiAgW3Byb3BlcnR5OiBzdHJpbmddOiBhbnk7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG5cbiAgLyoqIHRlbXAgc29sdXRpb24gc2luY2Ugbm8gbWV0aG9kIGFkZCBjbGFzc01hcCB0byBob3N0IGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvaXNzdWVzLzcyODkqL1xuICBzZXRDbGFzc01hcCgpOiB2b2lkIHtcbiAgICBjb25zdCBjbGFzc01hcCA9IHtcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tJHt0aGlzLm56U3Bhbn1gXTogaXNOb3ROaWwodGhpcy5uelNwYW4pLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1vcmRlci0ke3RoaXMubnpPcmRlcn1gXTogaXNOb3ROaWwodGhpcy5uek9yZGVyKSxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tb2Zmc2V0LSR7dGhpcy5uek9mZnNldH1gXTogaXNOb3ROaWwodGhpcy5uek9mZnNldCksXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LXB1bGwtJHt0aGlzLm56UHVsbH1gXTogaXNOb3ROaWwodGhpcy5uelB1bGwpLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1wdXNoLSR7dGhpcy5uelB1c2h9YF06IGlzTm90TmlsKHRoaXMubnpQdXNoKSxcbiAgICAgIC4uLnRoaXMuZ2VuZXJhdGVDbGFzcygpXG4gICAgfTtcbiAgICB0aGlzLm56VXBkYXRlSG9zdENsYXNzU2VydmljZS51cGRhdGVIb3N0Q2xhc3ModGhpcy5lbCwgY2xhc3NNYXApO1xuICB9XG5cbiAgZ2VuZXJhdGVDbGFzcygpOiBvYmplY3Qge1xuICAgIGNvbnN0IGxpc3RPZlNpemVJbnB1dE5hbWUgPSBbJ256WHMnLCAnbnpTbScsICduek1kJywgJ256TGcnLCAnbnpYbCcsICduelhYbCddO1xuICAgIGNvbnN0IGxpc3RDbGFzc01hcDogTmdDbGFzc0ludGVyZmFjZSA9IHt9O1xuICAgIGxpc3RPZlNpemVJbnB1dE5hbWUuZm9yRWFjaChuYW1lID0+IHtcbiAgICAgIGNvbnN0IHNpemVOYW1lID0gbmFtZS5yZXBsYWNlKCdueicsICcnKS50b0xvd2VyQ2FzZSgpO1xuICAgICAgaWYgKGlzTm90TmlsKHRoaXNbbmFtZV0pKSB7XG4gICAgICAgIGlmICh0eXBlb2YgdGhpc1tuYW1lXSA9PT0gJ251bWJlcicgfHwgdHlwZW9mIHRoaXNbbmFtZV0gPT09ICdzdHJpbmcnKSB7XG4gICAgICAgICAgbGlzdENsYXNzTWFwW2Ake3RoaXMucHJlZml4Q2xzfS0ke3NpemVOYW1lfS0ke3RoaXNbbmFtZV19YF0gPSB0cnVlO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGxpc3RDbGFzc01hcFtgJHt0aGlzLnByZWZpeENsc30tJHtzaXplTmFtZX0tJHt0aGlzW25hbWVdLnNwYW59YF0gPSB0aGlzW25hbWVdICYmIGlzTm90TmlsKHRoaXNbbmFtZV0uc3Bhbik7XG4gICAgICAgICAgbGlzdENsYXNzTWFwW2Ake3RoaXMucHJlZml4Q2xzfS0ke3NpemVOYW1lfS1wdWxsLSR7dGhpc1tuYW1lXS5wdWxsfWBdID1cbiAgICAgICAgICAgIHRoaXNbbmFtZV0gJiYgaXNOb3ROaWwodGhpc1tuYW1lXS5wdWxsKTtcbiAgICAgICAgICBsaXN0Q2xhc3NNYXBbYCR7dGhpcy5wcmVmaXhDbHN9LSR7c2l6ZU5hbWV9LXB1c2gtJHt0aGlzW25hbWVdLnB1c2h9YF0gPVxuICAgICAgICAgICAgdGhpc1tuYW1lXSAmJiBpc05vdE5pbCh0aGlzW25hbWVdLnB1c2gpO1xuICAgICAgICAgIGxpc3RDbGFzc01hcFtgJHt0aGlzLnByZWZpeENsc30tJHtzaXplTmFtZX0tb2Zmc2V0LSR7dGhpc1tuYW1lXS5vZmZzZXR9YF0gPVxuICAgICAgICAgICAgdGhpc1tuYW1lXSAmJiBpc05vdE5pbCh0aGlzW25hbWVdLm9mZnNldCk7XG4gICAgICAgICAgbGlzdENsYXNzTWFwW2Ake3RoaXMucHJlZml4Q2xzfS0ke3NpemVOYW1lfS1vcmRlci0ke3RoaXNbbmFtZV0ub3JkZXJ9YF0gPVxuICAgICAgICAgICAgdGhpc1tuYW1lXSAmJiBpc05vdE5pbCh0aGlzW25hbWVdLm9yZGVyKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0pO1xuICAgIHJldHVybiBsaXN0Q2xhc3NNYXA7XG4gIH1cblxuICBjb25zdHJ1Y3RvcihcbiAgICBwcml2YXRlIG56VXBkYXRlSG9zdENsYXNzU2VydmljZTogTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBAT3B0aW9uYWwoKSBASG9zdCgpIHB1YmxpYyBuelJvd0RpcmVjdGl2ZTogTnpSb3dEaXJlY3RpdmUsXG4gICAgcHVibGljIHJlbmRlcmVyOiBSZW5kZXJlcjJcbiAgKSB7fVxuXG4gIG5nT25DaGFuZ2VzKCk6IHZvaWQge1xuICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgfVxuXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uelJvd0RpcmVjdGl2ZSkge1xuICAgICAgdGhpcy5uelJvd0RpcmVjdGl2ZS5hY3R1YWxHdXR0ZXIkXG4gICAgICAgIC5waXBlKFxuICAgICAgICAgIHN0YXJ0V2l0aCh0aGlzLm56Um93RGlyZWN0aXZlLmFjdHVhbEd1dHRlciksXG4gICAgICAgICAgdGFrZVVudGlsKHRoaXMuZGVzdHJveSQpXG4gICAgICAgIClcbiAgICAgICAgLnN1YnNjcmliZShhY3R1YWxHdXR0ZXIgPT4ge1xuICAgICAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ3BhZGRpbmctbGVmdCcsIGAke2FjdHVhbEd1dHRlciAvIDJ9cHhgKTtcbiAgICAgICAgICB0aGlzLnJlbmRlcmVyLnNldFN0eWxlKHRoaXMuZWwsICdwYWRkaW5nLXJpZ2h0JywgYCR7YWN0dWFsR3V0dGVyIC8gMn1weGApO1xuICAgICAgICB9KTtcbiAgICB9XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3kkLm5leHQoKTtcbiAgICB0aGlzLmRlc3Ryb3kkLmNvbXBsZXRlKCk7XG4gIH1cbn1cbiJdfQ==