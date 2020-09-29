/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ComponentPortal, PortalInjector, TemplatePortal } from '@angular/cdk/portal';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Injector, Input, TemplateRef, Type, ViewContainerRef, ViewEncapsulation } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { Subscription } from 'rxjs';
import { simpleEmptyImage, NZ_EMPTY_COMPONENT_NAME } from './nz-empty-config';
import { NzEmptyService } from './nz-empty.service';
var NzEmbedEmptyComponent = /** @class */ (function () {
    function NzEmbedEmptyComponent(emptyService, sanitizer, viewContainerRef, cdr, injector) {
        this.emptyService = emptyService;
        this.sanitizer = sanitizer;
        this.viewContainerRef = viewContainerRef;
        this.cdr = cdr;
        this.injector = injector;
        this.contentType = 'string';
        // tslint:disable-line:no-any
        this.defaultSvg = this.sanitizer.bypassSecurityTrustResourceUrl(simpleEmptyImage);
        this.size = '';
        this.subs_ = new Subscription();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    NzEmbedEmptyComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzComponentName) {
            this.size = this.getEmptySize(changes.nzComponentName.currentValue);
        }
        if (changes.specificContent && !changes.specificContent.isFirstChange()) {
            this.content = changes.specificContent.currentValue;
            this.renderEmpty();
        }
    };
    /**
     * @return {?}
     */
    NzEmbedEmptyComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var userContent_ = this.emptyService.userDefaultContent$.subscribe((/**
         * @param {?} content
         * @return {?}
         */
        function (content) {
            _this.content = _this.specificContent || content;
            _this.renderEmpty();
        }));
        this.subs_.add(userContent_);
    };
    /**
     * @return {?}
     */
    NzEmbedEmptyComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.subs_.unsubscribe();
    };
    /**
     * @private
     * @param {?} componentName
     * @return {?}
     */
    NzEmbedEmptyComponent.prototype.getEmptySize = /**
     * @private
     * @param {?} componentName
     * @return {?}
     */
    function (componentName) {
        switch (componentName) {
            case 'table':
            case 'list':
                return 'normal';
            case 'select':
            case 'tree-select':
            case 'cascader':
            case 'transfer':
                return 'small';
            default:
                return '';
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzEmbedEmptyComponent.prototype.renderEmpty = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var content = this.content;
        if (typeof content === 'string') {
            this.contentType = 'string';
        }
        else if (content instanceof TemplateRef) {
            /** @type {?} */
            var context = (/** @type {?} */ ({ $implicit: this.nzComponentName }));
            this.contentType = 'template';
            this.contentPortal = new TemplatePortal(content, this.viewContainerRef, context);
        }
        else if (content instanceof Type) {
            /** @type {?} */
            var context = new WeakMap([[NZ_EMPTY_COMPONENT_NAME, this.nzComponentName]]);
            /** @type {?} */
            var injector = new PortalInjector(this.injector, context);
            this.contentType = 'component';
            this.contentPortal = new ComponentPortal(content, this.viewContainerRef, injector);
        }
        else {
            this.contentType = 'string';
            this.contentPortal = undefined;
        }
        this.cdr.markForCheck();
    };
    NzEmbedEmptyComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-embed-empty',
                    template: "<ng-container *ngIf=\"!content && specificContent !== null\" [ngSwitch]=\"size\">\n  <nz-empty *ngSwitchCase=\"'normal'\" class=\"ant-empty-normal\" [nzNotFoundImage]=\"defaultSvg\"></nz-empty>\n  <nz-empty *ngSwitchCase=\"'small'\" class=\"ant-empty-small\" [nzNotFoundImage]=\"defaultSvg\"></nz-empty>\n  <nz-empty *ngSwitchDefault></nz-empty>\n</ng-container>\n<ng-container *ngIf=\"content\">\n  <ng-template *ngIf=\"contentType !== 'string'\" [cdkPortalOutlet]=\"contentPortal\"></ng-template>\n  <ng-container *ngIf=\"contentType === 'string'\">\n    {{ content }}\n  </ng-container>\n</ng-container>\n"
                }] }
    ];
    /** @nocollapse */
    NzEmbedEmptyComponent.ctorParameters = function () { return [
        { type: NzEmptyService },
        { type: DomSanitizer },
        { type: ViewContainerRef },
        { type: ChangeDetectorRef },
        { type: Injector }
    ]; };
    NzEmbedEmptyComponent.propDecorators = {
        nzComponentName: [{ type: Input }],
        specificContent: [{ type: Input }]
    };
    return NzEmbedEmptyComponent;
}());
export { NzEmbedEmptyComponent };
if (false) {
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.nzComponentName;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.specificContent;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.content;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.contentType;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.contentPortal;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.defaultSvg;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.size;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.subs_;
    /** @type {?} */
    NzEmbedEmptyComponent.prototype.emptyService;
    /**
     * @type {?}
     * @private
     */
    NzEmbedEmptyComponent.prototype.sanitizer;
    /**
     * @type {?}
     * @private
     */
    NzEmbedEmptyComponent.prototype.viewContainerRef;
    /**
     * @type {?}
     * @private
     */
    NzEmbedEmptyComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzEmbedEmptyComponent.prototype.injector;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZW1iZWQtZW1wdHkuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImVtcHR5L256LWVtYmVkLWVtcHR5LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGVBQWUsRUFBVSxjQUFjLEVBQUUsY0FBYyxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFDOUYsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFFBQVEsRUFDUixLQUFLLEVBS0wsV0FBVyxFQUNYLElBQUksRUFDSixnQkFBZ0IsRUFDaEIsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSwyQkFBMkIsQ0FBQztBQUN6RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBRXBDLE9BQU8sRUFBRSxnQkFBZ0IsRUFBcUMsdUJBQXVCLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNqSCxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFFcEQ7SUFpQkUsK0JBQ1MsWUFBNEIsRUFDM0IsU0FBdUIsRUFDdkIsZ0JBQWtDLEVBQ2xDLEdBQXNCLEVBQ3RCLFFBQWtCO1FBSm5CLGlCQUFZLEdBQVosWUFBWSxDQUFnQjtRQUMzQixjQUFTLEdBQVQsU0FBUyxDQUFjO1FBQ3ZCLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBa0I7UUFDbEMsUUFBRyxHQUFILEdBQUcsQ0FBbUI7UUFDdEIsYUFBUSxHQUFSLFFBQVEsQ0FBVTtRQVg1QixnQkFBVyxHQUF3QyxRQUFRLENBQUM7O1FBRTVELGVBQVUsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDLDhCQUE4QixDQUFDLGdCQUFnQixDQUFDLENBQUM7UUFDN0UsU0FBSSxHQUFnQixFQUFFLENBQUM7UUFDdkIsVUFBSyxHQUFHLElBQUksWUFBWSxFQUFFLENBQUM7SUFReEIsQ0FBQzs7Ozs7SUFFSiwyQ0FBVzs7OztJQUFYLFVBQVksT0FBc0I7UUFDaEMsSUFBSSxPQUFPLENBQUMsZUFBZSxFQUFFO1lBQzNCLElBQUksQ0FBQyxJQUFJLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxPQUFPLENBQUMsZUFBZSxDQUFDLFlBQVksQ0FBQyxDQUFDO1NBQ3JFO1FBRUQsSUFBSSxPQUFPLENBQUMsZUFBZSxJQUFJLENBQUMsT0FBTyxDQUFDLGVBQWUsQ0FBQyxhQUFhLEVBQUUsRUFBRTtZQUN2RSxJQUFJLENBQUMsT0FBTyxHQUFHLE9BQU8sQ0FBQyxlQUFlLENBQUMsWUFBWSxDQUFDO1lBQ3BELElBQUksQ0FBQyxXQUFXLEVBQUUsQ0FBQztTQUNwQjtJQUNILENBQUM7Ozs7SUFFRCx3Q0FBUTs7O0lBQVI7UUFBQSxpQkFPQzs7WUFOTyxZQUFZLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxtQkFBbUIsQ0FBQyxTQUFTOzs7O1FBQUMsVUFBQSxPQUFPO1lBQzFFLEtBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSSxDQUFDLGVBQWUsSUFBSSxPQUFPLENBQUM7WUFDL0MsS0FBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1FBQ3JCLENBQUMsRUFBQztRQUVGLElBQUksQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLFlBQVksQ0FBQyxDQUFDO0lBQy9CLENBQUM7Ozs7SUFFRCwyQ0FBVzs7O0lBQVg7UUFDRSxJQUFJLENBQUMsS0FBSyxDQUFDLFdBQVcsRUFBRSxDQUFDO0lBQzNCLENBQUM7Ozs7OztJQUVPLDRDQUFZOzs7OztJQUFwQixVQUFxQixhQUFxQjtRQUN4QyxRQUFRLGFBQWEsRUFBRTtZQUNyQixLQUFLLE9BQU8sQ0FBQztZQUNiLEtBQUssTUFBTTtnQkFDVCxPQUFPLFFBQVEsQ0FBQztZQUNsQixLQUFLLFFBQVEsQ0FBQztZQUNkLEtBQUssYUFBYSxDQUFDO1lBQ25CLEtBQUssVUFBVSxDQUFDO1lBQ2hCLEtBQUssVUFBVTtnQkFDYixPQUFPLE9BQU8sQ0FBQztZQUNqQjtnQkFDRSxPQUFPLEVBQUUsQ0FBQztTQUNiO0lBQ0gsQ0FBQzs7Ozs7SUFFTywyQ0FBVzs7OztJQUFuQjs7WUFDUSxPQUFPLEdBQUcsSUFBSSxDQUFDLE9BQU87UUFFNUIsSUFBSSxPQUFPLE9BQU8sS0FBSyxRQUFRLEVBQUU7WUFDL0IsSUFBSSxDQUFDLFdBQVcsR0FBRyxRQUFRLENBQUM7U0FDN0I7YUFBTSxJQUFJLE9BQU8sWUFBWSxXQUFXLEVBQUU7O2dCQUNuQyxPQUFPLEdBQUcsbUJBQUEsRUFBRSxTQUFTLEVBQUUsSUFBSSxDQUFDLGVBQWUsRUFBRSxFQUFPO1lBQzFELElBQUksQ0FBQyxXQUFXLEdBQUcsVUFBVSxDQUFDO1lBQzlCLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxjQUFjLENBQUMsT0FBTyxFQUFFLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxPQUFPLENBQUMsQ0FBQztTQUNsRjthQUFNLElBQUksT0FBTyxZQUFZLElBQUksRUFBRTs7Z0JBQzVCLE9BQU8sR0FBRyxJQUFJLE9BQU8sQ0FBQyxDQUFDLENBQUMsdUJBQXVCLEVBQUUsSUFBSSxDQUFDLGVBQWUsQ0FBQyxDQUFDLENBQUM7O2dCQUN4RSxRQUFRLEdBQUcsSUFBSSxjQUFjLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRSxPQUFPLENBQUM7WUFDM0QsSUFBSSxDQUFDLFdBQVcsR0FBRyxXQUFXLENBQUM7WUFDL0IsSUFBSSxDQUFDLGFBQWEsR0FBRyxJQUFJLGVBQWUsQ0FBQyxPQUFPLEVBQUUsSUFBSSxDQUFDLGdCQUFnQixFQUFFLFFBQVEsQ0FBQyxDQUFDO1NBQ3BGO2FBQU07WUFDTCxJQUFJLENBQUMsV0FBVyxHQUFHLFFBQVEsQ0FBQztZQUM1QixJQUFJLENBQUMsYUFBYSxHQUFHLFNBQVMsQ0FBQztTQUNoQztRQUVELElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Z0JBcEZGLFNBQVMsU0FBQztvQkFDVCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLFFBQVEsRUFBRSxnQkFBZ0I7b0JBQzFCLDRtQkFBOEM7aUJBQy9DOzs7O2dCQVBRLGNBQWM7Z0JBSmQsWUFBWTtnQkFIbkIsZ0JBQWdCO2dCQVZoQixpQkFBaUI7Z0JBRWpCLFFBQVE7OztrQ0F3QlAsS0FBSztrQ0FDTCxLQUFLOztJQTZFUiw0QkFBQztDQUFBLEFBckZELElBcUZDO1NBL0VZLHFCQUFxQjs7O0lBQ2hDLGdEQUFpQzs7SUFDakMsZ0RBQStDOztJQUUvQyx3Q0FBK0I7O0lBQy9CLDRDQUE0RDs7SUFDNUQsOENBQTRCOztJQUM1QiwyQ0FBNkU7O0lBQzdFLHFDQUF1Qjs7SUFDdkIsc0NBQTJCOztJQUd6Qiw2Q0FBbUM7Ozs7O0lBQ25DLDBDQUErQjs7Ozs7SUFDL0IsaURBQTBDOzs7OztJQUMxQyxvQ0FBOEI7Ozs7O0lBQzlCLHlDQUEwQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IENvbXBvbmVudFBvcnRhbCwgUG9ydGFsLCBQb3J0YWxJbmplY3RvciwgVGVtcGxhdGVQb3J0YWwgfSBmcm9tICdAYW5ndWxhci9jZGsvcG9ydGFsJztcbmltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBJbmplY3RvcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFRlbXBsYXRlUmVmLFxuICBUeXBlLFxuICBWaWV3Q29udGFpbmVyUmVmLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IERvbVNhbml0aXplciB9IGZyb20gJ0Bhbmd1bGFyL3BsYXRmb3JtLWJyb3dzZXInO1xuaW1wb3J0IHsgU3Vic2NyaXB0aW9uIH0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7IHNpbXBsZUVtcHR5SW1hZ2UsIE56RW1wdHlDdXN0b21Db250ZW50LCBOekVtcHR5U2l6ZSwgTlpfRU1QVFlfQ09NUE9ORU5UX05BTUUgfSBmcm9tICcuL256LWVtcHR5LWNvbmZpZyc7XG5pbXBvcnQgeyBOekVtcHR5U2VydmljZSB9IGZyb20gJy4vbnotZW1wdHkuc2VydmljZSc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1lbWJlZC1lbXB0eScsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1lbWJlZC1lbXB0eS5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpFbWJlZEVtcHR5Q29tcG9uZW50IGltcGxlbWVudHMgT25DaGFuZ2VzLCBPbkluaXQsIE9uRGVzdHJveSB7XG4gIEBJbnB1dCgpIG56Q29tcG9uZW50TmFtZTogc3RyaW5nO1xuICBASW5wdXQoKSBzcGVjaWZpY0NvbnRlbnQ6IE56RW1wdHlDdXN0b21Db250ZW50O1xuXG4gIGNvbnRlbnQ/OiBOekVtcHR5Q3VzdG9tQ29udGVudDtcbiAgY29udGVudFR5cGU6ICdjb21wb25lbnQnIHwgJ3RlbXBsYXRlJyB8ICdzdHJpbmcnID0gJ3N0cmluZyc7XG4gIGNvbnRlbnRQb3J0YWw/OiBQb3J0YWw8YW55PjsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcbiAgZGVmYXVsdFN2ZyA9IHRoaXMuc2FuaXRpemVyLmJ5cGFzc1NlY3VyaXR5VHJ1c3RSZXNvdXJjZVVybChzaW1wbGVFbXB0eUltYWdlKTtcbiAgc2l6ZTogTnpFbXB0eVNpemUgPSAnJztcbiAgc3Vic18gPSBuZXcgU3Vic2NyaXB0aW9uKCk7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHVibGljIGVtcHR5U2VydmljZTogTnpFbXB0eVNlcnZpY2UsXG4gICAgcHJpdmF0ZSBzYW5pdGl6ZXI6IERvbVNhbml0aXplcixcbiAgICBwcml2YXRlIHZpZXdDb250YWluZXJSZWY6IFZpZXdDb250YWluZXJSZWYsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByaXZhdGUgaW5qZWN0b3I6IEluamVjdG9yXG4gICkge31cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpDb21wb25lbnROYW1lKSB7XG4gICAgICB0aGlzLnNpemUgPSB0aGlzLmdldEVtcHR5U2l6ZShjaGFuZ2VzLm56Q29tcG9uZW50TmFtZS5jdXJyZW50VmFsdWUpO1xuICAgIH1cblxuICAgIGlmIChjaGFuZ2VzLnNwZWNpZmljQ29udGVudCAmJiAhY2hhbmdlcy5zcGVjaWZpY0NvbnRlbnQuaXNGaXJzdENoYW5nZSgpKSB7XG4gICAgICB0aGlzLmNvbnRlbnQgPSBjaGFuZ2VzLnNwZWNpZmljQ29udGVudC5jdXJyZW50VmFsdWU7XG4gICAgICB0aGlzLnJlbmRlckVtcHR5KCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgY29uc3QgdXNlckNvbnRlbnRfID0gdGhpcy5lbXB0eVNlcnZpY2UudXNlckRlZmF1bHRDb250ZW50JC5zdWJzY3JpYmUoY29udGVudCA9PiB7XG4gICAgICB0aGlzLmNvbnRlbnQgPSB0aGlzLnNwZWNpZmljQ29udGVudCB8fCBjb250ZW50O1xuICAgICAgdGhpcy5yZW5kZXJFbXB0eSgpO1xuICAgIH0pO1xuXG4gICAgdGhpcy5zdWJzXy5hZGQodXNlckNvbnRlbnRfKTtcbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuc3Vic18udW5zdWJzY3JpYmUoKTtcbiAgfVxuXG4gIHByaXZhdGUgZ2V0RW1wdHlTaXplKGNvbXBvbmVudE5hbWU6IHN0cmluZyk6IE56RW1wdHlTaXplIHtcbiAgICBzd2l0Y2ggKGNvbXBvbmVudE5hbWUpIHtcbiAgICAgIGNhc2UgJ3RhYmxlJzpcbiAgICAgIGNhc2UgJ2xpc3QnOlxuICAgICAgICByZXR1cm4gJ25vcm1hbCc7XG4gICAgICBjYXNlICdzZWxlY3QnOlxuICAgICAgY2FzZSAndHJlZS1zZWxlY3QnOlxuICAgICAgY2FzZSAnY2FzY2FkZXInOlxuICAgICAgY2FzZSAndHJhbnNmZXInOlxuICAgICAgICByZXR1cm4gJ3NtYWxsJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiAnJztcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHJlbmRlckVtcHR5KCk6IHZvaWQge1xuICAgIGNvbnN0IGNvbnRlbnQgPSB0aGlzLmNvbnRlbnQ7XG5cbiAgICBpZiAodHlwZW9mIGNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICB0aGlzLmNvbnRlbnRUeXBlID0gJ3N0cmluZyc7XG4gICAgfSBlbHNlIGlmIChjb250ZW50IGluc3RhbmNlb2YgVGVtcGxhdGVSZWYpIHtcbiAgICAgIGNvbnN0IGNvbnRleHQgPSB7ICRpbXBsaWNpdDogdGhpcy5uekNvbXBvbmVudE5hbWUgfSBhcyBhbnk7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gICAgICB0aGlzLmNvbnRlbnRUeXBlID0gJ3RlbXBsYXRlJztcbiAgICAgIHRoaXMuY29udGVudFBvcnRhbCA9IG5ldyBUZW1wbGF0ZVBvcnRhbChjb250ZW50LCB0aGlzLnZpZXdDb250YWluZXJSZWYsIGNvbnRleHQpO1xuICAgIH0gZWxzZSBpZiAoY29udGVudCBpbnN0YW5jZW9mIFR5cGUpIHtcbiAgICAgIGNvbnN0IGNvbnRleHQgPSBuZXcgV2Vha01hcChbW05aX0VNUFRZX0NPTVBPTkVOVF9OQU1FLCB0aGlzLm56Q29tcG9uZW50TmFtZV1dKTtcbiAgICAgIGNvbnN0IGluamVjdG9yID0gbmV3IFBvcnRhbEluamVjdG9yKHRoaXMuaW5qZWN0b3IsIGNvbnRleHQpO1xuICAgICAgdGhpcy5jb250ZW50VHlwZSA9ICdjb21wb25lbnQnO1xuICAgICAgdGhpcy5jb250ZW50UG9ydGFsID0gbmV3IENvbXBvbmVudFBvcnRhbChjb250ZW50LCB0aGlzLnZpZXdDb250YWluZXJSZWYsIGluamVjdG9yKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5jb250ZW50VHlwZSA9ICdzdHJpbmcnO1xuICAgICAgdGhpcy5jb250ZW50UG9ydGFsID0gdW5kZWZpbmVkO1xuICAgIH1cblxuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG59XG4iXX0=