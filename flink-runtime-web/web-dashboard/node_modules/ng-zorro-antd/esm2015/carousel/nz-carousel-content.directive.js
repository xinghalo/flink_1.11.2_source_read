/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Directive, ElementRef, Renderer2 } from '@angular/core';
import { isNotNil } from '../core/util/check';
export class NzCarouselContentDirective {
    /**
     * @param {?} elementRef
     * @param {?} renderer
     */
    constructor(elementRef, renderer) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.el = this.elementRef.nativeElement;
        this._active = false;
        this._width = 0;
        this._fadeMode = false;
        renderer.addClass(elementRef.nativeElement, 'slick-slide');
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set width(value) {
        this._width = value;
        this.renderer.setStyle(this.el, 'width', `${this.width}px`);
    }
    /**
     * @return {?}
     */
    get width() {
        return this._width;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set left(value) {
        this._left = value;
        if (isNotNil(this.left)) {
            this.renderer.setStyle(this.el, 'left', `${this.left}px`);
        }
        else {
            this.renderer.removeStyle(this.el, 'left');
        }
    }
    /**
     * @return {?}
     */
    get left() {
        return this._left;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set top(value) {
        this._top = value;
        if (isNotNil(this.top)) {
            this.renderer.setStyle(this.el, 'top', `${this.top}px`);
        }
        else {
            this.renderer.removeStyle(this.el, 'top');
        }
    }
    /**
     * @return {?}
     */
    get top() {
        return this._top;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isActive(value) {
        this._active = value;
        this.updateOpacity();
        if (this.isActive) {
            this.renderer.addClass(this.el, 'slick-active');
        }
        else {
            this.renderer.removeClass(this.el, 'slick-active');
        }
    }
    /**
     * @return {?}
     */
    get isActive() {
        return this._active;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set fadeMode(value) {
        this._fadeMode = value;
        if (this.fadeMode) {
            this.renderer.setStyle(this.el, 'position', 'relative');
        }
        else {
            this.renderer.removeStyle(this.el, 'position');
        }
        this.updateOpacity();
    }
    /**
     * @return {?}
     */
    get fadeMode() {
        return this._fadeMode;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.renderer.setStyle(this.el, 'transition', 'opacity 500ms ease');
    }
    /**
     * @private
     * @return {?}
     */
    updateOpacity() {
        if (this.fadeMode) {
            this.renderer.setStyle(this.el, 'opacity', this.isActive ? 1 : 0);
        }
    }
}
NzCarouselContentDirective.decorators = [
    { type: Directive, args: [{
                selector: '[nz-carousel-content]'
            },] }
];
/** @nocollapse */
NzCarouselContentDirective.ctorParameters = () => [
    { type: ElementRef },
    { type: Renderer2 }
];
if (false) {
    /** @type {?} */
    NzCarouselContentDirective.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype._active;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype._width;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype._left;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype._top;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype._fadeMode;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzCarouselContentDirective.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2Fyb3VzZWwtY29udGVudC5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY2Fyb3VzZWwvbnotY2Fyb3VzZWwtY29udGVudC5kaXJlY3RpdmUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxTQUFTLEVBQUUsVUFBVSxFQUFVLFNBQVMsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUV6RSxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFLOUMsTUFBTSxPQUFPLDBCQUEwQjs7Ozs7SUF3RXJDLFlBQW9CLFVBQXNCLEVBQVUsUUFBbUI7UUFBbkQsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUFVLGFBQVEsR0FBUixRQUFRLENBQVc7UUF2RXZFLE9BQUUsR0FBZ0IsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUM7UUFFeEMsWUFBTyxHQUFHLEtBQUssQ0FBQztRQUNoQixXQUFNLEdBQVcsQ0FBQyxDQUFDO1FBR25CLGNBQVMsR0FBRyxLQUFLLENBQUM7UUFrRXhCLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxhQUFhLENBQUMsQ0FBQztJQUM3RCxDQUFDOzs7OztJQWpFRCxJQUFJLEtBQUssQ0FBQyxLQUFhO1FBQ3JCLElBQUksQ0FBQyxNQUFNLEdBQUcsS0FBSyxDQUFDO1FBQ3BCLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxFQUFFLEVBQUUsT0FBTyxFQUFFLEdBQUcsSUFBSSxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUM7SUFDOUQsQ0FBQzs7OztJQUVELElBQUksS0FBSztRQUNQLE9BQU8sSUFBSSxDQUFDLE1BQU0sQ0FBQztJQUNyQixDQUFDOzs7OztJQUVELElBQUksSUFBSSxDQUFDLEtBQW9CO1FBQzNCLElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO1FBQ25CLElBQUksUUFB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QUMzQiwyQ0FBNkI7Ozs7O0lBQzdCLDBDQUE0Qjs7Ozs7SUFDNUIsK0NBQTBCOzs7OztJQWlFZCxnREFBOEI7Ozs7O0lBQUUsOENBQTJCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRGlyZWN0aXZlLCBFbGVtZW50UmVmLCBPbkluaXQsIFJlbmRlcmVyMiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBpc05vdE5pbCB9IGZyb20gJy4uL2NvcmUvdXRpbC9jaGVjayc7XG5cbkBEaXJlY3RpdmUoe1xuICBzZWxlY3RvcjogJ1tuei1jYXJvdXNlbC1jb250ZW50XSdcbn0pXG5leHBvcnQgY2xhc3MgTnpDYXJvdXNlbENvbnRlbnREaXJlY3RpdmUgaW1wbGVtZW50cyBPbkluaXQge1xuICBlbDogSFRNTEVsZW1lbnQgPSB0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudDtcblxuICBwcml2YXRlIF9hY3RpdmUgPSBmYWxzZTtcbiAgcHJpdmF0ZSBfd2lkdGg6IG51bWJlciA9IDA7XG4gIHByaXZhdGUgX2xlZnQ6IG51bWJlciB8IG51bGw7XG4gIHByaXZhdGUgX3RvcDogbnVtYmVyIHwgbnVsbDtcbiAgcHJpdmF0ZSBfZmFkZU1vZGUgPSBmYWxzZTtcblxuICBzZXQgd2lkdGgodmFsdWU6IG51bWJlcikge1xuICAgIHRoaXMuX3dpZHRoID0gdmFsdWU7XG4gICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsLCAnd2lkdGgnLCBgJHt0aGlzLndpZHRofXB4YCk7XG4gIH1cblxuICBnZXQgd2lkdGgoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5fd2lkdGg7XG4gIH1cblxuICBzZXQgbGVmdCh2YWx1ZTogbnVtYmVyIHwgbnVsbCkge1xuICAgIHRoaXMuX2xlZnQgPSB2YWx1ZTtcbiAgICBpZiAoaXNOb3ROaWwodGhpcy5sZWZ0KSkge1xuICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsLCAnbGVmdCcsIGAke3RoaXMubGVmdH1weGApO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZVN0eWxlKHRoaXMuZWwsICdsZWZ0Jyk7XG4gICAgfVxuICB9XG5cbiAgZ2V0IGxlZnQoKTogbnVtYmVyIHwgbnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuX2xlZnQ7XG4gIH1cblxuICBzZXQgdG9wKHZhbHVlOiBudW1iZXIgfCBudWxsKSB7XG4gICAgdGhpcy5fdG9wID0gdmFsdWU7XG4gICAgaWYgKGlzTm90TmlsKHRoaXMudG9wKSkge1xuICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsLCAndG9wJywgYCR7dGhpcy50b3B9cHhgKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5yZW5kZXJlci5yZW1vdmVTdHlsZSh0aGlzLmVsLCAndG9wJyk7XG4gICAgfVxuICB9XG5cbiAgZ2V0IHRvcCgpOiBudW1iZXIgfCBudWxsIHtcbiAgICByZXR1cm4gdGhpcy5fdG9wO1xuICB9XG5cbiAgc2V0IGlzQWN0aXZlKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5fYWN0aXZlID0gdmFsdWU7XG4gICAgdGhpcy51cGRhdGVPcGFjaXR5KCk7XG4gICAgaWYgKHRoaXMuaXNBY3RpdmUpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuYWRkQ2xhc3ModGhpcy5lbCwgJ3NsaWNrLWFjdGl2ZScpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZUNsYXNzKHRoaXMuZWwsICdzbGljay1hY3RpdmUnKTtcbiAgICB9XG4gIH1cblxuICBnZXQgaXNBY3RpdmUoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2FjdGl2ZTtcbiAgfVxuXG4gIHNldCBmYWRlTW9kZSh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2ZhZGVNb2RlID0gdmFsdWU7XG4gICAgaWYgKHRoaXMuZmFkZU1vZGUpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ3Bvc2l0aW9uJywgJ3JlbGF0aXZlJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlU3R5bGUodGhpcy5lbCwgJ3Bvc2l0aW9uJyk7XG4gICAgfVxuICAgIHRoaXMudXBkYXRlT3BhY2l0eSgpO1xuICB9XG5cbiAgZ2V0IGZhZGVNb2RlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9mYWRlTW9kZTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZiwgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnc2xpY2stc2xpZGUnKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ3RyYW5zaXRpb24nLCAnb3BhY2l0eSA1MDBtcyBlYXNlJyk7XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZU9wYWNpdHkoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuZmFkZU1vZGUpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ29wYWNpdHknLCB0aGlzLmlzQWN0aXZlID8gMSA6IDApO1xuICAgIH1cbiAgfVxufVxuIl19