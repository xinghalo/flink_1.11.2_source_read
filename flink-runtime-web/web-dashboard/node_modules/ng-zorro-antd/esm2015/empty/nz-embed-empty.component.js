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
export class NzEmbedEmptyComponent {
    /**
     * @param {?} emptyService
     * @param {?} sanitizer
     * @param {?} viewContainerRef
     * @param {?} cdr
     * @param {?} injector
     */
    constructor(emptyService, sanitizer, viewContainerRef, cdr, injector) {
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
    ngOnChanges(changes) {
        if (changes.nzComponentName) {
            this.size = this.getEmptySize(changes.nzComponentName.currentValue);
        }
        if (changes.specificContent && !changes.specificContent.isFirstChange()) {
            this.content = changes.specificContent.currentValue;
            this.renderEmpty();
        }
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        /** @type {?} */
        const userContent_ = this.emptyService.userDefaultContent$.subscribe((/**
         * @param {?} content
         * @return {?}
         */
        content => {
            this.content = this.specificContent || content;
            this.renderEmpty();
        }));
        this.subs_.add(userContent_);
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.subs_.unsubscribe();
    }
    /**
     * @private
     * @param {?} componentName
     * @return {?}
     */
    getEmptySize(componentName) {
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
    }
    /**
     * @private
     * @return {?}
     */
    renderEmpty() {
        /** @type {?} */
        const content = this.content;
        if (typeof content === 'string') {
            this.contentType = 'string';
        }
        else if (content instanceof TemplateRef) {
            /** @type {?} */
            const context = (/** @type {?} */ ({ $implicit: this.nzComponentName }));
            this.contentType = 'template';
            this.contentPortal = new TemplatePortal(content, this.viewContainerRef, context);
        }
        else if (content instanceof Type) {
            /** @type {?} */
            const context = new WeakMap([[NZ_EMPTY_COMPONENT_NAME, this.nzComponentName]]);
            /** @type {?} */
            const injector = new PortalInjector(this.injector, context);
            this.contentType = 'component';
            this.contentPortal = new ComponentPortal(content, this.viewContainerRef, injector);
        }
        else {
            this.contentType = 'string';
            this.contentPortal = undefined;
        }
        this.cdr.markForCheck();
    }
}
NzEmbedEmptyComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                selector: 'nz-embed-empty',
                template: "<ng-container *ngIf=\"!content && specificContent !== null\" [ngSwitch]=\"size\">\n  <nz-empty *ngSwitchCase=\"'normal'\" class=\"ant-empty-normal\" [nzNotFoundImage]=\"defaultSvg\"></nz-empty>\n  <nz-empty *ngSwitchCase=\"'small'\" class=\"ant-empty-small\" [nzNotFoundImage]=\"defaultSvg\"></nz-empty>\n  <nz-empty *ngSwitchDefault></nz-empty>\n</ng-container>\n<ng-container *ngIf=\"content\">\n  <ng-template *ngIf=\"contentType !== 'string'\" [cdkPortalOutlet]=\"contentPortal\"></ng-template>\n  <ng-container *ngIf=\"contentType === 'string'\">\n    {{ content }}\n  </ng-container>\n</ng-container>\n"
            }] }
];
/** @nocollapse */
NzEmbedEmptyComponent.ctorParameters = () => [
    { type: NzEmptyService },
    { type: DomSanitizer },
    { type: ViewContainerRef },
    { type: ChangeDetectorRef },
    { type: Injector }
];
NzEmbedEmptyComponent.propDecorators = {
    nzComponentName: [{ type: Input }],
    specificContent: [{ type: Input }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZW1iZWQtZW1wdHkuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImVtcHR5L256LWVtYmVkLWVtcHR5LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGVBQWUsRUFBVSxjQUFjLEVBQUUsY0FBYyxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFDOUYsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFFBQVEsRUFDUixLQUFLLEVBS0wsV0FBVyxFQUNYLElBQUksRUFDSixnQkFBZ0IsRUFDaEIsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSwyQkFBMkIsQ0FBQztBQUN6RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBRXBDLE9BQU8sRUFBRSxnQkFBZ0IsRUFBcUMsdUJBQXVCLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNqSCxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFRcEQsTUFBTSxPQUFPLHFCQUFxQjs7Ozs7Ozs7SUFXaEMsWUFDUyxZQUE0QixFQUMzQixTQUF1QixFQUN2QixnQkFBa0MsRUFDbEMsR0FBc0IsRUFDdEIsUUFBa0I7UUFKbkIsaUJBQVksR0FBWixZQUFZLENBQWdCO1FBQzNCLGNBQVMsR0FBVCxTQUFTLENBQWM7UUFDdkIscUJBQWdCLEdBQWhCLGdCQUFnQixDQUFrQjtRQUNsQyxRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQUN0QixhQUFRLEdBQVIsUUFBUSxDQUFVO1FBWDVCLGdCQUFXLEdBQXdDLFFBQVEsQ0FBQzs7UUFFNUQsZUFBVSxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsOEJBQThCLENBQUMsZ0JBQWdCLENBQUMsQ0FBQztRQUM3RSxTQUFJLEdBQWdCLEVBQUUsQ0FBQztRQUN2QixVQUFLLEdBQUcsSUFBSSxZQUFZLEVBQUUsQ0FBQztJQVF4QixDQUFDOzs7OztJQUVKLFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxlQUFlLEVBQUU7WUFDM0IsSUFBSSxDQUFDLElBQUksR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLE9BQU8sQ0FBQyxlQUFlLENBQUMsWUFBWSxDQUFDLENBQUM7U0FDckU7UUFFRCxJQUFJLE9BQU8sQ0FBQyxlQUFlLElBQUksQ0FBQyxPQUFPLENBQUMsZUFBZSxDQUFDLGFBQWEsRUFBRSxFQUFFO1lBQ3ZFLElBQUksQ0FBQyxPQUFPLEdBQUcsT0FBTyxDQUFDLGVBQWUsQ0FBQyxZQUFZLENBQUM7WUFDcEQsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1NBQ3BCO0lBQ0gsQ0FBQzs7OztJQUVELFFBQVE7O2NBQ0EsWUFBWSxHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsbUJBQW1CLENBQUMsU0FBUzs7OztRQUFDLE9BQU8sQ0FBQyxFQUFFO1lBQzdFLElBQUksQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDLGVBQWUsSUFBSSxPQUFPLENBQUM7WUFDL0MsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1FBQ3JCLENBQUMsRUFBQztRQUVGLElBQUksQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLFlBQVksQ0FBQyxDQUFDO0lBQy9CLENBQUM7Ozs7SUFFRCxXQUFXO1FBQ1QsSUFBSSxDQUFDLEtBQUssQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUMzQixDQUFDOzs7Ozs7SUFFTyxZQUFZLENBQUMsYUFBcUI7UUFDeEMsUUFBUSxhQUFhLEVBQUU7WUFDckIsS0FBSyxPQUFPLENBQUM7WUFDYixLQUFLLE1BQU07Z0JBQ1QsT0FBTyxRQUFRLENBQUM7WUFDbEIsS0FBSyxRQUFRLENBQUM7WUFDZCxLQUFLLGFBQWEsQ0FBQztZQUNuQixLQUFLLFVBQVUsQ0FBQztZQUNoQixLQUFLLFVBQVU7Z0JBQ2IsT0FBTyxPQUFPLENBQUM7WUFDakI7Z0JBQ0UsT0FBTyxFQUFFLENBQUM7U0FDYjtJQUNILENBQUM7Ozs7O0lBRU8sV0FBVzs7Y0FDWCxPQUFPLEdBQUcsSUFBSSxDQUFDLE9BQU87UUFFNUIsSUFBSSxPQUFPLE9BQU8sS0FBSyxRQUFRLEVBQUU7WUFDL0IsSUFBSSxDQUFDLFdBQVcsR0FBRyxRQUFRLENBQUM7U0FDN0I7YUFBTSxJQUFJLE9BQU8sWUFBWSxXQUFXLEVBQUU7O2tCQUNuQyxPQUFPLEdBQUcsbUJBQUEsRUFBRSxTQUFTLEVBQUUsSUFBSSxDQUFDLGVBQWUsRUFBRSxFQUFPO1lBQzFELElBQUksQ0FBQyxXQUFXLEdBQUcsVUFBVSxDQUFDO1lBQzlCLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxjQUFjLENBQUMsT0FBTyxFQUFFLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxPQUFPLENBQUMsQ0FBQztTQUNsRjthQUFNLElBQUksT0FBTyxZQUFZLElBQUksRUFBRTs7a0JBQzVCLE9BQU8sR0FBRyxJQUFJLE9BQU8sQ0FBQyxDQUFDLENBQUMsdUJBQXVCLEVBQUUsSUFBSSxDQUFDLGVBQWUsQ0FBQyxDQUFDLENBQUM7O2tCQUN4RSxRQUFRLEdBQUcsSUFBSSxjQUFjLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRSxPQUFPLENBQUM7WUFDM0QsSUFBSSxDQUFDLFdBQVcsR0FBRyxXQUFXLENBQUM7WUFDL0IsSUFBSSxDQUFDLGFBQWEsR0FBRyxJQUFJLGVBQWUsQ0FBQyxPQUFPLEVBQUUsSUFBSSxDQUFDLGdCQUFnQixFQUFFLFFBQVEsQ0FBQyxDQUFDO1NBQ3BGO2FBQU07WUFDTCxJQUFJLENBQUMsV0FBVyxHQUFHLFFBQVEsQ0FBQztZQUM1QixJQUFJLENBQUMsYUFBYSxHQUFHLFNBQVMsQ0FBQztTQUNoQztRQUVELElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7O1lBcEZGLFNBQVMsU0FBQztnQkFDVCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLFFBQVEsRUFBRSxnQkFBZ0I7Z0JBQzFCLDRtQkFBOEM7YUFDL0M7Ozs7WUFQUSxjQUFjO1lBSmQsWUFBWTtZQUhuQixnQkFBZ0I7WUFWaEIsaUJBQWlCO1lBRWpCLFFBQVE7Ozs4QkF3QlAsS0FBSzs4QkFDTCxLQUFLOzs7O0lBRE4sZ0RBQWlDOztJQUNqQyxnREFBK0M7O0lBRS9DLHdDQUErQjs7SUFDL0IsNENBQTREOztJQUM1RCw4Q0FBNEI7O0lBQzVCLDJDQUE2RTs7SUFDN0UscUNBQXVCOztJQUN2QixzQ0FBMkI7O0lBR3pCLDZDQUFtQzs7Ozs7SUFDbkMsMENBQStCOzs7OztJQUMvQixpREFBMEM7Ozs7O0lBQzFDLG9DQUE4Qjs7Ozs7SUFDOUIseUNBQTBCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29tcG9uZW50UG9ydGFsLCBQb3J0YWwsIFBvcnRhbEluamVjdG9yLCBUZW1wbGF0ZVBvcnRhbCB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9wb3J0YWwnO1xuaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEluamVjdG9yLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFR5cGUsXG4gIFZpZXdDb250YWluZXJSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgRG9tU2FuaXRpemVyIH0gZnJvbSAnQGFuZ3VsYXIvcGxhdGZvcm0tYnJvd3Nlcic7XG5pbXBvcnQgeyBTdWJzY3JpcHRpb24gfSBmcm9tICdyeGpzJztcblxuaW1wb3J0IHsgc2ltcGxlRW1wdHlJbWFnZSwgTnpFbXB0eUN1c3RvbUNvbnRlbnQsIE56RW1wdHlTaXplLCBOWl9FTVBUWV9DT01QT05FTlRfTkFNRSB9IGZyb20gJy4vbnotZW1wdHktY29uZmlnJztcbmltcG9ydCB7IE56RW1wdHlTZXJ2aWNlIH0gZnJvbSAnLi9uei1lbXB0eS5zZXJ2aWNlJztcblxuQENvbXBvbmVudCh7XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBzZWxlY3RvcjogJ256LWVtYmVkLWVtcHR5JyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWVtYmVkLWVtcHR5LmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOekVtYmVkRW1wdHlDb21wb25lbnQgaW1wbGVtZW50cyBPbkNoYW5nZXMsIE9uSW5pdCwgT25EZXN0cm95IHtcbiAgQElucHV0KCkgbnpDb21wb25lbnROYW1lOiBzdHJpbmc7XG4gIEBJbnB1dCgpIHNwZWNpZmljQ29udGVudDogTnpFbXB0eUN1c3RvbUNvbnRlbnQ7XG5cbiAgY29udGVudD86IE56RW1wdHlDdXN0b21Db250ZW50O1xuICBjb250ZW50VHlwZTogJ2NvbXBvbmVudCcgfCAndGVtcGxhdGUnIHwgJ3N0cmluZycgPSAnc3RyaW5nJztcbiAgY29udGVudFBvcnRhbD86IFBvcnRhbDxhbnk+OyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuICBkZWZhdWx0U3ZnID0gdGhpcy5zYW5pdGl6ZXIuYnlwYXNzU2VjdXJpdHlUcnVzdFJlc291cmNlVXJsKHNpbXBsZUVtcHR5SW1hZ2UpO1xuICBzaXplOiBOekVtcHR5U2l6ZSA9ICcnO1xuICBzdWJzXyA9IG5ldyBTdWJzY3JpcHRpb24oKTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICBwdWJsaWMgZW1wdHlTZXJ2aWNlOiBOekVtcHR5U2VydmljZSxcbiAgICBwcml2YXRlIHNhbml0aXplcjogRG9tU2FuaXRpemVyLFxuICAgIHByaXZhdGUgdmlld0NvbnRhaW5lclJlZjogVmlld0NvbnRhaW5lclJlZixcbiAgICBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgcHJpdmF0ZSBpbmplY3RvcjogSW5qZWN0b3JcbiAgKSB7fVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uekNvbXBvbmVudE5hbWUpIHtcbiAgICAgIHRoaXMuc2l6ZSA9IHRoaXMuZ2V0RW1wdHlTaXplKGNoYW5nZXMubnpDb21wb25lbnROYW1lLmN1cnJlbnRWYWx1ZSk7XG4gICAgfVxuXG4gICAgaWYgKGNoYW5nZXMuc3BlY2lmaWNDb250ZW50ICYmICFjaGFuZ2VzLnNwZWNpZmljQ29udGVudC5pc0ZpcnN0Q2hhbmdlKCkpIHtcbiAgICAgIHRoaXMuY29udGVudCA9IGNoYW5nZXMuc3BlY2lmaWNDb250ZW50LmN1cnJlbnRWYWx1ZTtcbiAgICAgIHRoaXMucmVuZGVyRW1wdHkoKTtcbiAgICB9XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICBjb25zdCB1c2VyQ29udGVudF8gPSB0aGlzLmVtcHR5U2VydmljZS51c2VyRGVmYXVsdENvbnRlbnQkLnN1YnNjcmliZShjb250ZW50ID0+IHtcbiAgICAgIHRoaXMuY29udGVudCA9IHRoaXMuc3BlY2lmaWNDb250ZW50IHx8IGNvbnRlbnQ7XG4gICAgICB0aGlzLnJlbmRlckVtcHR5KCk7XG4gICAgfSk7XG5cbiAgICB0aGlzLnN1YnNfLmFkZCh1c2VyQ29udGVudF8pO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5zdWJzXy51bnN1YnNjcmliZSgpO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRFbXB0eVNpemUoY29tcG9uZW50TmFtZTogc3RyaW5nKTogTnpFbXB0eVNpemUge1xuICAgIHN3aXRjaCAoY29tcG9uZW50TmFtZSkge1xuICAgICAgY2FzZSAndGFibGUnOlxuICAgICAgY2FzZSAnbGlzdCc6XG4gICAgICAgIHJldHVybiAnbm9ybWFsJztcbiAgICAgIGNhc2UgJ3NlbGVjdCc6XG4gICAgICBjYXNlICd0cmVlLXNlbGVjdCc6XG4gICAgICBjYXNlICdjYXNjYWRlcic6XG4gICAgICBjYXNlICd0cmFuc2Zlcic6XG4gICAgICAgIHJldHVybiAnc21hbGwnO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuICcnO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgcmVuZGVyRW1wdHkoKTogdm9pZCB7XG4gICAgY29uc3QgY29udGVudCA9IHRoaXMuY29udGVudDtcblxuICAgIGlmICh0eXBlb2YgY29udGVudCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHRoaXMuY29udGVudFR5cGUgPSAnc3RyaW5nJztcbiAgICB9IGVsc2UgaWYgKGNvbnRlbnQgaW5zdGFuY2VvZiBUZW1wbGF0ZVJlZikge1xuICAgICAgY29uc3QgY29udGV4dCA9IHsgJGltcGxpY2l0OiB0aGlzLm56Q29tcG9uZW50TmFtZSB9IGFzIGFueTsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcbiAgICAgIHRoaXMuY29udGVudFR5cGUgPSAndGVtcGxhdGUnO1xuICAgICAgdGhpcy5jb250ZW50UG9ydGFsID0gbmV3IFRlbXBsYXRlUG9ydGFsKGNvbnRlbnQsIHRoaXMudmlld0NvbnRhaW5lclJlZiwgY29udGV4dCk7XG4gICAgfSBlbHNlIGlmIChjb250ZW50IGluc3RhbmNlb2YgVHlwZSkge1xuICAgICAgY29uc3QgY29udGV4dCA9IG5ldyBXZWFrTWFwKFtbTlpfRU1QVFlfQ09NUE9ORU5UX05BTUUsIHRoaXMubnpDb21wb25lbnROYW1lXV0pO1xuICAgICAgY29uc3QgaW5qZWN0b3IgPSBuZXcgUG9ydGFsSW5qZWN0b3IodGhpcy5pbmplY3RvciwgY29udGV4dCk7XG4gICAgICB0aGlzLmNvbnRlbnRUeXBlID0gJ2NvbXBvbmVudCc7XG4gICAgICB0aGlzLmNvbnRlbnRQb3J0YWwgPSBuZXcgQ29tcG9uZW50UG9ydGFsKGNvbnRlbnQsIHRoaXMudmlld0NvbnRhaW5lclJlZiwgaW5qZWN0b3IpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmNvbnRlbnRUeXBlID0gJ3N0cmluZyc7XG4gICAgICB0aGlzLmNvbnRlbnRQb3J0YWwgPSB1bmRlZmluZWQ7XG4gICAgfVxuXG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cbn1cbiJdfQ==