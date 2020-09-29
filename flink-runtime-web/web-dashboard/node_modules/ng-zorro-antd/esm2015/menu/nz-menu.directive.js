/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ContentChildren, Directive, ElementRef, EventEmitter, Input, Optional, Output, QueryList, SkipSelf } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util/convert';
import { NzMenuDropdownService } from '../dropdown/nz-menu-dropdown.service';
import { NzMenuItemDirective } from './nz-menu-item.directive';
import { NzMenuMenuService } from './nz-menu-menu.service';
import { NzMenuService } from './nz-menu.service';
import { NzSubMenuComponent } from './nz-submenu.component';
/**
 * @param {?} dropService
 * @param {?} menuService
 * @return {?}
 */
export function NzMenuFactory(dropService, menuService) {
    return dropService ? dropService : menuService;
}
export class NzMenuDirective {
    /**
     * @param {?} elementRef
     * @param {?} nzMenuService
     * @param {?} nzUpdateHostClassService
     */
    constructor(elementRef, nzMenuService, nzUpdateHostClassService) {
        this.elementRef = elementRef;
        this.nzMenuService = nzMenuService;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.destroy$ = new Subject();
        this.listOfOpenedNzSubMenuComponent = [];
        this.nzInlineIndent = 24;
        this.nzTheme = 'light';
        this.nzMode = 'vertical';
        this.nzInDropDown = false;
        this.nzInlineCollapsed = false;
        this.nzSelectable = !this.nzMenuService.isInDropDown;
        this.nzClick = new EventEmitter();
    }
    /**
     * @return {?}
     */
    updateInlineCollapse() {
        if (this.listOfNzMenuItemDirective) {
            if (this.nzInlineCollapsed) {
                this.listOfOpenedNzSubMenuComponent = this.listOfNzSubMenuComponent.filter((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                submenu => submenu.nzOpen));
                this.listOfNzSubMenuComponent.forEach((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                submenu => submenu.setOpenState(false)));
                this.nzMode = 'vertical';
            }
            else {
                this.listOfOpenedNzSubMenuComponent.forEach((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                submenu => submenu.setOpenState(true)));
                this.listOfOpenedNzSubMenuComponent = [];
                this.nzMode = this.cacheMode;
            }
            this.nzMenuService.setMode(this.nzMode);
        }
    }
    /**
     * @return {?}
     */
    setClassMap() {
        /** @type {?} */
        const prefixName = this.nzMenuService.isInDropDown ? 'ant-dropdown-menu' : 'ant-menu';
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, {
            [`${prefixName}`]: true,
            [`${prefixName}-root`]: true,
            [`${prefixName}-${this.nzTheme}`]: true,
            [`${prefixName}-${this.nzMode}`]: true,
            [`${prefixName}-inline-collapsed`]: this.nzInlineCollapsed
        });
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.setClassMap();
        this.nzMenuService.menuItemClick$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} menu
         * @return {?}
         */
        menu => {
            this.nzClick.emit(menu);
            if (this.nzSelectable) {
                this.listOfNzMenuItemDirective.forEach((/**
                 * @param {?} item
                 * @return {?}
                 */
                item => item.setSelectedState(item === menu)));
            }
        }));
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.cacheMode = this.nzMode;
        this.updateInlineCollapse();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzInlineCollapsed) {
            this.updateInlineCollapse();
        }
        if (changes.nzInlineIndent) {
            this.nzMenuService.setInlineIndent(this.nzInlineIndent);
        }
        if (changes.nzInDropDown) {
            this.nzMenuService.isInDropDown = this.nzInDropDown;
        }
        if (changes.nzTheme) {
            this.nzMenuService.setTheme(this.nzTheme);
        }
        if (changes.nzMode) {
            this.nzMenuService.setMode(this.nzMode);
            if (!changes.nzMode.isFirstChange() && this.listOfNzSubMenuComponent) {
                this.listOfNzSubMenuComponent.forEach((/**
                 * @param {?} submenu
                 * @return {?}
                 */
                submenu => submenu.setOpenState(false)));
            }
        }
        if (changes.nzTheme || changes.nzMode || changes.nzInlineCollapsed) {
            this.setClassMap();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
}
NzMenuDirective.decorators = [
    { type: Directive, args: [{
                selector: '[nz-menu]',
                providers: [
                    NzUpdateHostClassService,
                    NzMenuMenuService,
                    {
                        provide: NzMenuService,
                        useFactory: NzMenuFactory,
                        deps: [[new SkipSelf(), new Optional(), NzMenuDropdownService], NzMenuMenuService]
                    }
                ]
            },] }
];
/** @nocollapse */
NzMenuDirective.ctorParameters = () => [
    { type: ElementRef },
    { type: NzMenuService },
    { type: NzUpdateHostClassService }
];
NzMenuDirective.propDecorators = {
    listOfNzMenuItemDirective: [{ type: ContentChildren, args: [NzMenuItemDirective, { descendants: true },] }],
    listOfNzSubMenuComponent: [{ type: ContentChildren, args: [NzSubMenuComponent, { descendants: true },] }],
    nzInlineIndent: [{ type: Input }],
    nzTheme: [{ type: Input }],
    nzMode: [{ type: Input }],
    nzInDropDown: [{ type: Input }],
    nzInlineCollapsed: [{ type: Input }],
    nzSelectable: [{ type: Input }],
    nzClick: [{ type: Output }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzMenuDirective.prototype, "nzInDropDown", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzMenuDirective.prototype, "nzInlineCollapsed", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzMenuDirective.prototype, "nzSelectable", void 0);
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.cacheMode;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.listOfOpenedNzSubMenuComponent;
    /** @type {?} */
    NzMenuDirective.prototype.listOfNzMenuItemDirective;
    /** @type {?} */
    NzMenuDirective.prototype.listOfNzSubMenuComponent;
    /** @type {?} */
    NzMenuDirective.prototype.nzInlineIndent;
    /** @type {?} */
    NzMenuDirective.prototype.nzTheme;
    /** @type {?} */
    NzMenuDirective.prototype.nzMode;
    /** @type {?} */
    NzMenuDirective.prototype.nzInDropDown;
    /** @type {?} */
    NzMenuDirective.prototype.nzInlineCollapsed;
    /** @type {?} */
    NzMenuDirective.prototype.nzSelectable;
    /** @type {?} */
    NzMenuDirective.prototype.nzClick;
    /** @type {?} */
    NzMenuDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.nzMenuService;
    /**
     * @type {?}
     * @private
     */
    NzMenuDirective.prototype.nzUpdateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVudS5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibWVudS9uei1tZW51LmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCxlQUFlLEVBQ2YsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUlMLFFBQVEsRUFDUixNQUFNLEVBQ04sU0FBUyxFQUVULFFBQVEsRUFDVCxNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUMzQyxPQUFPLEVBQUUsd0JBQXdCLEVBQUUsTUFBTSw0Q0FBNEMsQ0FBQztBQUV0RixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEQsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sc0NBQXNDLENBQUM7QUFDN0UsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDL0QsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDM0QsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLG1CQUFtQixDQUFDO0FBQ2xELE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLHdCQUF3QixDQUFDOzs7Ozs7QUFFNUQsTUFBTSxVQUFVLGFBQWEsQ0FBQyxXQUFrQyxFQUFFLFdBQThCO0lBQzlGLE9BQU8sV0FBVyxDQUFDLENBQUMsQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLFdBQVcsQ0FBQztBQUNqRCxDQUFDO0FBY0QsTUFBTSxPQUFPLGVBQWU7Ozs7OztJQTBDMUIsWUFDUyxVQUFzQixFQUNyQixhQUE0QixFQUM1Qix3QkFBa0Q7UUFGbkQsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUNyQixrQkFBYSxHQUFiLGFBQWEsQ0FB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dGVyQ29udGVudEluaXQsXG4gIENvbnRlbnRDaGlsZHJlbixcbiAgRGlyZWN0aXZlLFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uRGVzdHJveSxcbiAgT25Jbml0LFxuICBPcHRpb25hbCxcbiAgT3V0cHV0LFxuICBRdWVyeUxpc3QsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFNraXBTZWxmXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IE56RGlyZWN0aW9uVkhJVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvZGlyZWN0aW9uJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56TWVudURyb3Bkb3duU2VydmljZSB9IGZyb20gJy4uL2Ryb3Bkb3duL256LW1lbnUtZHJvcGRvd24uc2VydmljZSc7XG5pbXBvcnQgeyBOek1lbnVJdGVtRGlyZWN0aXZlIH0gZnJvbSAnLi9uei1tZW51LWl0ZW0uZGlyZWN0aXZlJztcbmltcG9ydCB7IE56TWVudU1lbnVTZXJ2aWNlIH0gZnJvbSAnLi9uei1tZW51LW1lbnUuc2VydmljZSc7XG5pbXBvcnQgeyBOek1lbnVTZXJ2aWNlIH0gZnJvbSAnLi9uei1tZW51LnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpTdWJNZW51Q29tcG9uZW50IH0gZnJvbSAnLi9uei1zdWJtZW51LmNvbXBvbmVudCc7XG5cbmV4cG9ydCBmdW5jdGlvbiBOek1lbnVGYWN0b3J5KGRyb3BTZXJ2aWNlOiBOek1lbnVEcm9wZG93blNlcnZpY2UsIG1lbnVTZXJ2aWNlOiBOek1lbnVNZW51U2VydmljZSk6IE56TWVudVNlcnZpY2Uge1xuICByZXR1cm4gZHJvcFNlcnZpY2UgPyBkcm9wU2VydmljZSA6IG1lbnVTZXJ2aWNlO1xufVxuXG5ARGlyZWN0aXZlKHtcbiAgc2VsZWN0b3I6ICdbbnotbWVudV0nLFxuICBwcm92aWRlcnM6IFtcbiAgICBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UsXG4gICAgTnpNZW51TWVudVNlcnZpY2UsXG4gICAge1xuICAgICAgcHJvdmlkZTogTnpNZW51U2VydmljZSxcbiAgICAgIHVzZUZhY3Rvcnk6IE56TWVudUZhY3RvcnksXG4gICAgICBkZXBzOiBbW25ldyBTa2lwU2VsZigpLCBuZXcgT3B0aW9uYWwoKSwgTnpNZW51RHJvcGRvd25TZXJ2aWNlXSwgTnpNZW51TWVudVNlcnZpY2VdXG4gICAgfVxuICBdXG59KVxuZXhwb3J0IGNsYXNzIE56TWVudURpcmVjdGl2ZSBpbXBsZW1lbnRzIEFmdGVyQ29udGVudEluaXQsIE9uSW5pdCwgT25DaGFuZ2VzLCBPbkRlc3Ryb3kge1xuICBwcml2YXRlIGRlc3Ryb3kkID0gbmV3IFN1YmplY3QoKTtcbiAgcHJpdmF0ZSBjYWNoZU1vZGU6IE56RGlyZWN0aW9uVkhJVHlwZTtcbiAgcHJpdmF0ZSBsaXN0T2ZPcGVuZWROelN1Yk1lbnVDb21wb25lbnQ6IE56U3ViTWVudUNvbXBvbmVudFtdID0gW107XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpNZW51SXRlbURpcmVjdGl2ZSwgeyBkZXNjZW5kYW50czogdHJ1ZSB9KSBsaXN0T2ZOek1lbnVJdGVtRGlyZWN0aXZlOiBRdWVyeUxpc3Q8XG4gICAgTnpNZW51SXRlbURpcmVjdGl2ZVxuICA+O1xuICBAQ29udGVudENoaWxkcmVuKE56U3ViTWVudUNvbXBvbmVudCwgeyBkZXNjZW5kYW50czogdHJ1ZSB9KSBsaXN0T2ZOelN1Yk1lbnVDb21wb25lbnQ6IFF1ZXJ5TGlzdDxOelN1Yk1lbnVDb21wb25lbnQ+O1xuICBASW5wdXQoKSBueklubGluZUluZGVudCA9IDI0O1xuICBASW5wdXQoKSBuelRoZW1lOiAnbGlnaHQnIHwgJ2RhcmsnID0gJ2xpZ2h0JztcbiAgQElucHV0KCkgbnpNb2RlOiBOekRpcmVjdGlvblZISVR5cGUgPSAndmVydGljYWwnO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpJbkRyb3BEb3duID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBueklubGluZUNvbGxhcHNlZCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTZWxlY3RhYmxlID0gIXRoaXMubnpNZW51U2VydmljZS5pc0luRHJvcERvd247XG4gIEBPdXRwdXQoKSByZWFkb25seSBuekNsaWNrID0gbmV3IEV2ZW50RW1pdHRlcjxOek1lbnVJdGVtRGlyZWN0aXZlPigpO1xuXG4gIHVwZGF0ZUlubGluZUNvbGxhcHNlKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmxpc3RPZk56TWVudUl0ZW1EaXJlY3RpdmUpIHtcbiAgICAgIGlmICh0aGlzLm56SW5saW5lQ29sbGFwc2VkKSB7XG4gICAgICAgIHRoaXMubGlzdE9mT3BlbmVkTnpTdWJNZW51Q29tcG9uZW50ID0gdGhpcy5saXN0T2ZOelN1Yk1lbnVDb21wb25lbnQuZmlsdGVyKHN1Ym1lbnUgPT4gc3VibWVudS5uek9wZW4pO1xuICAgICAgICB0aGlzLmxpc3RPZk56U3ViTWVudUNvbXBvbmVudC5mb3JFYWNoKHN1Ym1lbnUgPT4gc3VibWVudS5zZXRPcGVuU3RhdGUoZmFsc2UpKTtcbiAgICAgICAgdGhpcy5uek1vZGUgPSAndmVydGljYWwnO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5saXN0T2ZPcGVuZWROelN1Yk1lbnVDb21wb25lbnQuZm9yRWFjaChzdWJtZW51ID0+IHN1Ym1lbnUuc2V0T3BlblN0YXRlKHRydWUpKTtcbiAgICAgICAgdGhpcy5saXN0T2ZPcGVuZWROelN1Yk1lbnVDb21wb25lbnQgPSBbXTtcbiAgICAgICAgdGhpcy5uek1vZGUgPSB0aGlzLmNhY2hlTW9kZTtcbiAgICAgIH1cbiAgICAgIHRoaXMubnpNZW51U2VydmljZS5zZXRNb2RlKHRoaXMubnpNb2RlKTtcbiAgICB9XG4gIH1cblxuICBzZXRDbGFzc01hcCgpOiB2b2lkIHtcbiAgICBjb25zdCBwcmVmaXhOYW1lID0gdGhpcy5uek1lbnVTZXJ2aWNlLmlzSW5Ecm9wRG93biA/ICdhbnQtZHJvcGRvd24tbWVudScgOiAnYW50LW1lbnUnO1xuICAgIHRoaXMubnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLnVwZGF0ZUhvc3RDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwge1xuICAgICAgW2Ake3ByZWZpeE5hbWV9YF06IHRydWUsXG4gICAgICBbYCR7cHJlZml4TmFtZX0tcm9vdGBdOiB0cnVlLFxuICAgICAgW2Ake3ByZWZpeE5hbWV9LSR7dGhpcy5uelRoZW1lfWBdOiB0cnVlLFxuICAgICAgW2Ake3ByZWZpeE5hbWV9LSR7dGhpcy5uek1vZGV9YF06IHRydWUsXG4gICAgICBbYCR7cHJlZml4TmFtZX0taW5saW5lLWNvbGxhcHNlZGBdOiB0aGlzLm56SW5saW5lQ29sbGFwc2VkXG4gICAgfSk7XG4gIH1cblxuICBjb25zdHJ1Y3RvcihcbiAgICBwdWJsaWMgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIG56TWVudVNlcnZpY2U6IE56TWVudVNlcnZpY2UsXG4gICAgcHJpdmF0ZSBuelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2U6IE56VXBkYXRlSG9zdENsYXNzU2VydmljZVxuICApIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIHRoaXMubnpNZW51U2VydmljZS5tZW51SXRlbUNsaWNrJC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSkuc3Vic2NyaWJlKG1lbnUgPT4ge1xuICAgICAgdGhpcy5uekNsaWNrLmVtaXQobWVudSk7XG4gICAgICBpZiAodGhpcy5uelNlbGVjdGFibGUpIHtcbiAgICAgICAgdGhpcy5saXN0T2ZOek1lbnVJdGVtRGlyZWN0aXZlLmZvckVhY2goaXRlbSA9PiBpdGVtLnNldFNlbGVjdGVkU3RhdGUoaXRlbSA9PT0gbWVudSkpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIHRoaXMuY2FjaGVNb2RlID0gdGhpcy5uek1vZGU7XG4gICAgdGhpcy51cGRhdGVJbmxpbmVDb2xsYXBzZSgpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLm56SW5saW5lQ29sbGFwc2VkKSB7XG4gICAgICB0aGlzLnVwZGF0ZUlubGluZUNvbGxhcHNlKCk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56SW5saW5lSW5kZW50KSB7XG4gICAgICB0aGlzLm56TWVudVNlcnZpY2Uuc2V0SW5saW5lSW5kZW50KHRoaXMubnpJbmxpbmVJbmRlbnQpO1xuICAgIH1cbiAgICBpZiAoY2hhbmdlcy5uekluRHJvcERvd24pIHtcbiAgICAgIHRoaXMubnpNZW51U2VydmljZS5pc0luRHJvcERvd24gPSB0aGlzLm56SW5Ecm9wRG93bjtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpUaGVtZSkge1xuICAgICAgdGhpcy5uek1lbnVTZXJ2aWNlLnNldFRoZW1lKHRoaXMubnpUaGVtZSk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56TW9kZSkge1xuICAgICAgdGhpcy5uek1lbnVTZXJ2aWNlLnNldE1vZGUodGhpcy5uek1vZGUpO1xuICAgICAgaWYgKCFjaGFuZ2VzLm56TW9kZS5pc0ZpcnN0Q2hhbmdlKCkgJiYgdGhpcy5saXN0T2ZOelN1Yk1lbnVDb21wb25lbnQpIHtcbiAgICAgICAgdGhpcy5saXN0T2ZOelN1Yk1lbnVDb21wb25lbnQuZm9yRWFjaChzdWJtZW51ID0+IHN1Ym1lbnUuc2V0T3BlblN0YXRlKGZhbHNlKSk7XG4gICAgICB9XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56VGhlbWUgfHwgY2hhbmdlcy5uek1vZGUgfHwgY2hhbmdlcy5ueklubGluZUNvbGxhcHNlZCkge1xuICAgICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveSQubmV4dCgpO1xuICAgIHRoaXMuZGVzdHJveSQuY29tcGxldGUoKTtcbiAgfVxufVxuIl19